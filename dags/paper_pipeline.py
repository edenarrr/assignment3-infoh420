from datetime import datetime
from pathlib import Path

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
        papers_folder = Path(
            "/Users/eden/Library/CloudStorage/GoogleDrive-zukatry@gmail.com/My Drive/ResearchLibrary/papers"
        )

        pdf_files = list(papers_folder.glob("*.pdf"))

        for pdf in pdf_files:
            print(f"Found PDF: {pdf.name}")

        return [str(pdf) for pdf in pdf_files]

    find_papers()


paper_pipeline()