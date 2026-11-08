"""Check the setup before starting Airflow. No Discord message is sent."""
import os
from pathlib import Path
import sys
from openai import OpenAI
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dags'))
from paper_pipeline import get_paths

root, db = get_paths()
assert (root / 'papers').is_dir(), f'Missing papers folder: {root / "papers"}'
client = OpenAI(base_url=os.environ.get('LOCAL_LLM_BASE_URL', 'http://127.0.0.1:11434/v1/'),
                api_key=os.environ.get('LOCAL_LLM_API_KEY', 'ollama'), timeout=10, max_retries=0)
models = [model.id for model in client.models.list().data]
model = os.environ.get('LOCAL_LLM_MODEL', 'llama3.2:3b')
print('Available models:', ', '.join(models))
assert model in models, f'{model} is not available; update LOCAL_LLM_MODEL or load the model.'
assert os.environ.get('DISCORD_WEBHOOK_URL'), 'Set DISCORD_WEBHOOK_URL in .env.'
print('Paths and model configuration checked. Discord delivery and Drive sync need a real run.')
