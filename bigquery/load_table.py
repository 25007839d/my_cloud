from google.cloud import bigquery


client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = 'ritu-351906.dush01.emp12'

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('NAME', 'STRING'),
        bigquery.SchemaField('EMAIL', 'STRING'),
        bigquery.SchemaField('AGE', 'STRING'),

    ],
    skip_leading_rows=1,

    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://dushyant01/data.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))

