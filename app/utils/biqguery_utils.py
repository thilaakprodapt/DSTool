from google.cloud import bigquery
from google.oauth2 import service_account
PROJECT_ID = "cloud-practice-dev-2"

def get_static_bq_client(service_account_file: str):
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def get_bq_client_from_sa_file(service_account_path: str, project_id: str):
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    return bigquery.Client(credentials=credentials, project=project_id, location="US")


