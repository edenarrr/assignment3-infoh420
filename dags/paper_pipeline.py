from datetime import datetime
from pathlib import Path
import sqlite3

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
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS papers(
                    filename TEXT PRIMARY KEY,
                    title TEXT,
                    summary TEXT,
                    keywords TEXT,
                    category TEXT
                )
            """)

            rows =conn.execute("""
            SELECT filename FROM papers
            """).fetchall()

            for row in rows:
                processed_files.add(row[0])
            

        papers_folder = Path("/Users/eden/Library/CloudStorage/GoogleDrive-zukatry@gmail.com/My Drive/ResearchLibrary/papers")
        unprocessed_pdf_files = list(paper_pdf for paper_pdf in papers_folder.glob("*.pdf") if paper_pdf.name not in processed_files) # Gets all unprocessed files
        

        for pdf in unprocessed_pdf_files:
            print(f"Found new PDF: {pdf.name}")

        return [str(pdf) for pdf in unprocessed_pdf_files]

    find_papers()


paper_pipeline()