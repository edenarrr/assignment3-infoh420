from datetime import datetime
from pathlib import Path
import sqlite3
from pymupdf4llm import to_markdown
from openai import OpenAI


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

    @task
    def convert_to_markdown(pdf_path: str):
        markdown = to_markdown(pdf_path)
        #prints to debug
        #print(f"converted {Path(pdf_path).name}")
        #print(markdown[:500])
        return {"filename": pdf_path, "markdown": markdown}

    @task
    def generate_metadata(markdown: dict):
        client = OpenAI(
            base_url="http://127.0.0.1:11434/v1/",
            api_key="ollama",
        )

        markdown_text = markdown["markdown"] # bcs its a dictionary

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
        print(f"generated metadata: {json_out}")
        return {"filename": markdown["filename"],"metadata_json": json_out}

    @task
    def save_metadata_to_database():
        pass


    pdfs = find_papers()
    markdown_results = convert_to_markdown.expand(pdf_path=pdfs)
    metadata_results = generate_metadata.expand(markdown=markdown_results)



paper_pipeline()