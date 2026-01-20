from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel
from app.core.config import bq_client

def find_target_column(dataset,table):
    query = f"""
        SELECT *
        FROM `{dataset}.{table}`
        """
        
    df = bq_client.query(query).to_dataframe()

    # ----------------------------------
    # 2. Build column metadata
    # ----------------------------------
    metadata = {}

    for col in df.columns:
        metadata[col] = {
            "dtype": str(df[col].dtype),
            "unique_values": df[col].nunique(),
            "sample_values": df[col].dropna().astype(str).unique()[:5].tolist()
        }
    model = GenerativeModel("gemini-2.0-flash")

    prompt = f""" You are an expert data scientist.
            Your task: Identify the most likely target column from the dataset.
             Dataset columns:
               {df.columns.tolist()}
            Column metadata:
               {metadata}
            Rule: Identify the MOST LIKELY target column
            Return only the column_name 
        """
    response = model.generate_content(prompt)
    response_text=response.text.strip().replace(" ", "_")
    # print("response:",response_text)
    return response_text