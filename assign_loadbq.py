import os
from google.cloud import bigquery
credential_path = "C:\\Users\\Jeyakar Mondreti\\venv\\tesla-stock-analysis-b1dbc26f972d.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "tesla-stock-analysis.tesla.tslv9"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Open", "FLOAT"),
        bigquery.SchemaField("High", "FLOAT"),
        bigquery.SchemaField("Low", "FLOAT"),
        bigquery.SchemaField("Close", "FLOAT"),
        bigquery.SchemaField("Volume", "INTEGER"),
        bigquery.SchemaField("Adj_Close", "FLOAT")       
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://tesla-stock-exchange-storage/Tesla.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))