from datetime import datetime
from pathlib import Path
import sqlite3
from pymupdf4llm import to_markdown
from openai import OpenAI
import json
from html import escape

from airflow.sdk import dag, task


@dag(
    dag_id="paper_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["research-library"],
)
def paper_pipeline():

    @task
    def find_papers():
        project_root = Path(__file__).resolve().parents[1]
        db_path = project_root / "data" / "papers.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        processed_files = set() # Set that contains all files already processed and in the database

        #Creates the table in the DB and checks for already processed files so that we only process new files
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS papers(
                filename TEXT PRIMARY KEY,
                title TEXT,
                summary TEXT,
                keywords TEXT,
                category TEXT
            )
        """)

        rows =cur.execute("""SELECT filename FROM papers""").fetchall()

        conn.commit()
        conn.close()

        for row in rows:
            processed_files.add(row[0])
            

        papers_folder = Path("/Users/eden/Library/CloudStorage/GoogleDrive-zukatry@gmail.com/My Drive/ResearchLibrary/papers")
        unprocessed_pdf_files = list(paper_pdf for paper_pdf in papers_folder.glob("*.pdf") if paper_pdf.name not in processed_files) # Gets all unprocessed files

        for pdf in unprocessed_pdf_files:
            print(f"Found new PDF: {pdf.name}")

        return [str(pdf) for pdf in unprocessed_pdf_files]

    @task
    def convert_to_markdown(pdf_path: str):
        markdown = to_markdown(pdf_path)
        #prints to debug
        #print(f"converted {Path(pdf_path).name}")
        #print(markdown[:500])
        return {"filename": Path(pdf_path).name, "markdown": markdown}

    @task
    def generate_metadata(markdown: dict):
        client = OpenAI(
            base_url="http://127.0.0.1:11434/v1/",
            api_key="ollama",
        )

        markdown_text = markdown["markdown"] # bcs its a dictionary

        # Loop that retry once if parsing fails
        metadata_dict = dict()
        parsing_valid = False
        for attempt in range(2):
            parsing_valid = True
            response = client.chat.completions.create(
            model="llama3.2:3b",
            messages=[
                {
                    "role": "system",
                    "content": """
                                you analyze academic research papers.

                                return ONLY a valid JSON object with exactly four fields: title, summary, keywords, category.

                                -title: string
                                -summary: string (a short summary of the paper)
                                -keywords: array of a few strings
                                -category: string

                                do not invent information!
                                """
                },
                {
                    "role": "user",
                    "content": (
                        "Analyze this paper and generate its metadata like requested:\n"+ markdown_text[:6000]
                    )
                }
            ],
            response_format={"type": "json_object"},
            )

            json_out = response.choices[0].message.content

            try:
                metadata_dict = json.loads(json_out)
            except (json.JSONDecodeError, TypeError) as error:
                if attempt == 0:
                    print(f"Invalid json, retrying: {error}")
                    continue
                else:
                    raise
            
            # to verify if all required metadata fields are valid, if its the 2nd try, raise an error
            if not isinstance(metadata_dict, dict): # check if its a dictionary
                if attempt == 0:
                    continue
                else:
                    raise ValueError("Metadata must be a dictionary")
            required_fields = {"title": str, "summary": str,"keywords": list,"category": str }
            
            for field, expected_type in required_fields.items():
                if field not in metadata_dict:
                    if attempt == 0:
                        parsing_valid = False
                        break
                    else:
                        raise ValueError(f"Missing field in metadata json: {field}")
                if not isinstance(metadata_dict[field], expected_type):
                    if attempt == 0:
                        parsing_valid = False
                        break
                    else:
                        raise TypeError(f"Invalid type in metadata json for: {field}")
            if parsing_valid:
                for k in metadata_dict["keywords"]:
                    if not isinstance(k, str):
                        if attempt == 0:
                            parsing_valid = False
                            break
                        else:
                            raise TypeError("Keywords must be a list of strings")
            if parsing_valid:
                break

    
        print(f"generated metadata: {metadata_dict}")
        return {"filename": markdown["filename"],"metadata": metadata_dict}

    @task
    def save_metadata_to_database(metadata: dict):
        project_root = Path(__file__).resolve().parents[1]
        db_path = project_root / "data" / "papers.db"
        metadata_dict = metadata["metadata"]
        
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
                INSERT INTO papers (filename, title, summary, keywords, category)
                VALUES(?, ?, ?, ?, ?)
            """,
            (metadata["filename"], metadata_dict["title"], metadata_dict["summary"], json.dumps(metadata_dict["keywords"]), metadata_dict["category"]) # Here keyword are stored as json string
        )

        print(f"Saved metadata for {metadata['filename']}")

        conn.commit()
        conn.close()

    @task
    def generate_html():
        project_root = Path(__file__).resolve().parents[1]
        db_path = project_root / "data" / "papers.db"

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        papers_rows = cur.execute("""
            SELECT filename, title, summary, keywords, category FROM papers
        """).fetchall()
        conn.close()

        #print(f"found{len(papers_rows)} papers in database")

        html = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>Research Library</title>
                </head>
                <body>
                    <h1>Research Paper Library</h1>
            """

        for filename, title, summary, keywords, category in papers_rows:
            keywords_list = json.loads(keywords)
            html += f"""
                        <article>
                        <h2>Title: {escape(title)}</h2>
                        <p>Summary: {escape(summary)}</p>
                        <p>Keywords: {escape(", ".join(keywords_list))}</p>
                        <p>Category: {escape(category)}</p>
                        </article>
                    """

        html += """
                </body>
                </html>
                """

        output_folder = Path("/Users/eden/Library/CloudStorage/GoogleDrive-zukatry@gmail.com/My Drive/ResearchLibrary/output")
        output_folder.mkdir(parents=True, exist_ok=True)
        html_path = output_folder/"index.html"
        html_path.write_text(html)

    @task
    def notify_discord():
        ...



    pdfs = find_papers()
    markdown_results = convert_to_markdown.expand(pdf_path=pdfs)
    metadata_results = generate_metadata.expand(markdown=markdown_results)
    save_metadata_to_database.expand(metadata=metadata_results) >> generate_html()



paper_pipeline()