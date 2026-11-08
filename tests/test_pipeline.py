"""Integration tests with real conversion/SQLite and fake LLM/webhook HTTP replies."""
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from paper_pipeline import paper_pipeline_dag, check_metadata

METADATA = {"title": "A small data pipeline", "summary": "A synthetic document about a local study library.",
            "keywords": ["pipeline", "PDF"], "category": "Data Engineering"}


def call_task(name, *args):
    operator = paper_pipeline_dag.get_task(name)
    function = getattr(operator, "python_callable", None)
    if function is None:
        function = operator.partial_kwargs["python_callable"]
    return function(*args)


class FakeServices(BaseHTTPRequestHandler):
    replies = []
    requests = []
    notifications = []
    fail_webhook = False

    def log_message(self, *args):
        pass

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/v1/chat/completions":
            self.requests.append(payload)
            content = self.replies.pop(0) if self.replies else json.dumps(METADATA)
            result = {"id": "fixture", "object": "chat.completion", "created": 0,
                      "model": "test-fixture", "choices": [{"index": 0, "finish_reason": "stop",
                      "message": {"role": "assistant", "content": content}}]}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())
        elif self.path == "/webhook":
            self.send_response(503 if self.fail_webhook else 204)
            self.end_headers()
            if not self.fail_webhook:
                self.notifications.append(payload)
        else:
            self.send_response(404)
            self.end_headers()


def make_pdf(path):
    import pymupdf
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text((60, 65), "A small data pipeline", fontsize=18)
    page.insert_textbox((60, 100, 520, 700),
        "Synthetic test PDF, not a published paper.\n\n"
        "A scheduled workflow converts PDFs to Markdown. A local model extracts metadata, "
        "which is stored in SQLite. A static HTML page lets students filter papers by category.", fontsize=12)
    doc.save(path)
    doc.close()


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "synced/papers").mkdir(parents=True)
        FakeServices.replies, FakeServices.requests, FakeServices.notifications = [], [], []
        FakeServices.fail_webhook = False
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeServices)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        url = f"http://127.0.0.1:{self.server.server_port}"
        self.env = patch.dict(os.environ, {
            "RESEARCH_LIBRARY_DIR": str(self.root / "synced"),
            "PAPERS_DB": str(self.root / "local/papers.db"),
            "LOCAL_LLM_BASE_URL": url + "/v1", "LOCAL_LLM_MODEL": "test-fixture",
            "LOCAL_LLM_JSON_MODE": "1", "DISCORD_WEBHOOK_URL": url + "/webhook",
            "PDF_SETTLE_SECONDS": "0", "MAX_PAPER_CHARACTERS": "24000"})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def run_functions(self):
        found = call_task("find_papers")
        for pdf in found:
            md = call_task("convert_to_markdown", pdf)
            self.assertNotIn("markdown", md)
            self.assertLess(len(json.dumps(md)), 1000)
            metadata = call_task("generate_metadata", md)
            call_task("save_metadata_to_database", metadata)
        count = call_task("generate_html")
        sent = call_task("notify_discord")
        return len(found), count, sent

    def test_new_paper_unchanged_rerun_then_another_paper(self):
        make_pdf(self.root / "synced/papers/first.pdf")
        self.assertEqual(self.run_functions(), (1, 1, 1))
        self.assertEqual(self.run_functions(), (0, 1, 0))
        make_pdf(self.root / "synced/papers/second.PDF")
        self.assertEqual(self.run_functions(), (1, 2, 1))
        self.assertEqual(len(FakeServices.requests), 2)
        self.assertEqual(len(FakeServices.notifications), 2)

    def test_json_retry_once(self):
        path = self.root / "paper.md"
        path.write_text("A test paper")
        FakeServices.replies = ["invalid JSON", json.dumps(METADATA)]
        result = call_task("generate_metadata", {"filename": "p.pdf", "markdown_path": str(path)})
        self.assertEqual(result["metadata"], METADATA)
        self.assertEqual(len(FakeServices.requests), 2)

    def test_two_bad_responses_stop_without_saving(self):
        call_task("find_papers")
        path = self.root / "paper.md"
        path.write_text("A test paper")
        FakeServices.replies = ["{}", "[]"]
        with self.assertRaises(ValueError):
            call_task("generate_metadata", {"filename": "p.pdf", "markdown_path": str(path)})
        self.assertEqual(len(FakeServices.requests), 2)
        with sqlite3.connect(self.root / "local/papers.db") as con:
            self.assertEqual(con.execute("SELECT count(*) FROM papers").fetchone()[0], 0)

    def test_full_text_sent_and_context_limit_is_explicit(self):
        path = self.root / "long.md"
        text = "Paper content. " * 700 + "IMPORTANT_LAST_SENTENCE"
        path.write_text(text)
        document = {"filename": "long.pdf", "markdown_path": str(path)}
        call_task("generate_metadata", document)
        self.assertIn("IMPORTANT_LAST_SENTENCE", FakeServices.requests[0]["messages"][1]["content"])
        os.environ["MAX_PAPER_CHARACTERS"] = "1000"
        with self.assertRaisesRegex(ValueError, "not been truncated"):
            call_task("generate_metadata", document)
        self.assertEqual(len(FakeServices.requests), 1)

    def test_storage_retry_and_concurrent_inserts(self):
        call_task("find_papers")
        paper = {"filename": "one.pdf", "metadata": METADATA}
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: call_task("save_metadata_to_database", paper), range(4)))
        with sqlite3.connect(self.root / "local/papers.db") as con:
            self.assertEqual(con.execute("SELECT count(*) FROM papers").fetchone()[0], 1)

    def test_notification_failure_recovers_on_empty_scan(self):
        call_task("find_papers")
        call_task("save_metadata_to_database", {"filename": "p.pdf", "metadata": METADATA})
        call_task("generate_html")
        FakeServices.fail_webhook = True
        with self.assertRaises(RuntimeError):
            call_task("notify_discord")
        self.assertEqual(call_task("find_papers"), [])
        FakeServices.fail_webhook = False
        self.assertEqual(call_task("notify_discord"), 1)
        self.assertEqual(call_task("notify_discord"), 0)

    def test_migrate_original_database_without_old_notifications(self):
        (self.root / "local").mkdir()
        with sqlite3.connect(self.root / "local/papers.db") as con:
            con.execute("CREATE TABLE papers(filename TEXT PRIMARY KEY,title TEXT,summary TEXT,keywords TEXT,category TEXT)")
            con.execute("INSERT INTO papers VALUES(?,?,?,?,?)", ("old.pdf", "Old paper", "Old summary", "[]", "Other"))
        call_task("find_papers")
        self.assertEqual(call_task("notify_discord"), 0)
        call_task("save_metadata_to_database", {"filename": "new.pdf", "metadata": METADATA})
        self.assertEqual(call_task("notify_discord"), 1)
        with sqlite3.connect(self.root / "local/papers.db") as con:
            self.assertEqual(con.execute("SELECT count(*) FROM papers").fetchone()[0], 2)

    def test_empty_library_and_missing_webhook(self):
        self.assertEqual(self.run_functions(), (0, 0, 0))
        os.environ["DISCORD_WEBHOOK_URL"] = ""
        call_task("save_metadata_to_database", {"filename": "p.pdf", "metadata": METADATA})
        with self.assertRaises(ValueError):
            call_task("notify_discord")
        with sqlite3.connect(self.root / "local/papers.db") as con:
            self.assertEqual(con.execute("SELECT notified FROM papers").fetchone()[0], 0)

    def test_html_escaping_and_relative_pdf_link(self):
        call_task("find_papers")
        call_task("save_metadata_to_database", {"filename": "a #1.pdf", "metadata": {**METADATA, "title": "<script>x</script>"}})
        call_task("generate_html")
        page = (self.root / "synced/output/index.html").read_text()
        self.assertIn("&lt;script&gt;x&lt;/script&gt;", page)
        self.assertIn("../papers/a%20%231.pdf", page)

    def test_type_checks_and_database_location(self):
        with self.assertRaises(ValueError):
            check_metadata(json.dumps({**METADATA, "keywords": [3]}))
        os.environ["PAPERS_DB"] = str(self.root / "synced/papers.db")
        with self.assertRaises(ValueError):
            call_task("find_papers")

    def test_missing_papers_folder_is_reported(self):
        (self.root / "synced/papers").rmdir()
        with self.assertRaises(FileNotFoundError):
            call_task("find_papers")

    def test_dag_mapping_and_empty_run_dependencies(self):
        dag = paper_pipeline_dag
        self.assertEqual(dag.schedule, "@daily")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(set(dag.task_ids), {"find_papers", "convert_to_markdown", "generate_metadata",
                                           "save_metadata_to_database", "generate_html", "notify_discord"})
        self.assertEqual(dag.get_task("generate_html").upstream_task_ids, {"find_papers", "save_metadata_to_database"})
        self.assertEqual(dag.get_task("generate_html").trigger_rule.value, "none_failed_min_one_success")
        self.assertEqual(dag.get_task("notify_discord").upstream_task_ids, {"generate_html"})
        self.assertIn("Mapped", type(dag.get_task("convert_to_markdown")).__name__)


if __name__ == "__main__":
    unittest.main(verbosity=2)
