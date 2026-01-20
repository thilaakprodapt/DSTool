import os
from google.oauth2 import service_account
from google.cloud import bigquery, storage
import vertexai

# -----------------------------
# PROJECT CONFIG
# -----------------------------
# Get the project root directory (2 levels up from this config file)
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_THIS_DIR))

PROJECT_ID = "cloud-practice-dev-2"
FE_TABLE_ID = "cloud-practice-dev-2.DS_details.FE_details"
LEAK_TABLE_ID = "cloud-practice-dev-2.DS_details.Leakage_Detection_details"
SERVICE_ACCOUNT_FILE = os.path.join(_PROJECT_ROOT, "cloud-practice-dev-2-17b17664fe4e.json")  
schema_id = "DS_details"
WORKSPACE_details = "cloud-practice-dev-2.DS_details.Workspace_details"

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# -----------------------------
# INITIALIZE VERTEX AI
# -----------------------------
vertexai.init(
    project=PROJECT_ID,
    credentials=credentials,
    location="us-central1"
)

# -----------------------------
# CLIENTS
# -----------------------------
bq_client = bigquery.Client(
    project=PROJECT_ID,
    credentials=credentials
)

gcs_client = storage.Client(
    project=PROJECT_ID,
    credentials=credentials
)
