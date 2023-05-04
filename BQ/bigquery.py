# Create table and schema
from google.cloud import bigquery

if __name__ == '__main__':
    obj_clint= bigquery.Client()

    schema= [
        bigquery.SchemaField('dept_id','INTEGER'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('salary', 'INTEGER'),
        bigquery.SchemaField('city', 'STRING'),
        bigquery.SchemaField('state', 'STRING'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('date', 'DATE')
        ]


    table = bigquery.Table('ritu-351906.bq_bwt.department',schema)
    table.time_partitioning = bigquery.TimePartitioning(field='date')
    table = obj_clint.create_table(table)

    print("create table successflly{}".format(table.table_id))

'''nested table or schem'''
# from google.cloud import bigquery
#
# if __name__ == '__main__':
#     obj_clint= bigquery.Client()
#
#     schema= [
#         bigquery.SchemaField('dept_id','INTEGER'),
#         bigquery.SchemaField('name', 'STRING'),
#         bigquery.SchemaField('salary', 'INTEGER'),
#         bigquery.SchemaField('city', 'STRING'),
#         bigquery.SchemaField('state', 'STRING'),
#         bigquery.SchemaField('country', 'RECORD',mode='REPEATED',fields=[
#                                bigquery.SchemaField('zone','STRING')
#
#                                  ]),
#
#         ]
#
#
#     table = bigquery.Table('ritu-351906.bq_bwt.department2',schema)
#     table.clustering_fields=['dept_id']
#     table = obj_clint.create_table(table)
#
#     print("create table successflly{}".format(table.table_id))

'''Create Dataset'''
# from google.cloud import bigquery
# a=bigquery.Client
# # Construct a BigQuery client object.
# client = bigquery.Client()
# # dataset_id = "{}.bwt_python".format(client.project) no  required
# dataset_id=client.dataset('bwt_python1','ritu-351906')
# # dataset = bigquery.Dataset(dataset_id) no required
# dataset_id.location = "US"
#
# dataset = client.create_dataset(dataset_id, timeout=30)  # Make an API request.
# print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

'''COPY DATASET'''
# from google.cloud import bigquery_datatransfer
#
# transfer_client = bigquery_datatransfer.DataTransferServiceClient()
#
# destination_project_id = "ritu-351906"
# destination_dataset_id = "bwt_python"
# source_project_id = "ritu-351906"
# source_dataset_id = "bq_bwt"
# transfer_config = bigquery_datatransfer.TransferConfig(
#     destination_dataset_id=destination_dataset_id,
#     display_name="Your Dataset Copy Name",
#     data_source_id="cross_region_copy",
#     params={
#         "source_project_id": source_project_id,
#         "source_dataset_id": source_dataset_id,
#     },
#
# )
# transfer_config = transfer_client.create_transfer_config(
#     parent=transfer_client.common_project_path(destination_project_id),
#     transfer_config=transfer_config,
# )
# print(f"Created transfer config: {transfer_config.name}")

'''list daeaset'''

# from google.cloud import bigquery
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# datasets = list(client.list_datasets())  # Make an API request.
# project = client.list_datasets('charming-shield-350913')
#
# if datasets:
#     print("Datasets in project {}:".format(project))
#     for dataset in datasets:
#         print("\t{}".format(dataset.dataset_id))
# else:
#     print("{} project does not contain any datasets.".format(project))

'''delete dataset'''


# from google.cloud import bigquery
# a = bigquery.Client
#
#
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # TODO(developer): Set model_id to the ID of the model to fetch.
# dataset_id = 'ritu-351906.create_table_schema'
#
# client.delete_dataset(
#     dataset_id, delete_contents=True, not_found_ok=True)  # Make an API request.
#
# print("Deleted dataset '{}'.".format(dataset_id))


# '''Create a view'''
# from google.cloud import bigquery
#
# client = bigquery.Client()
#
# view_id = "ritu-351906.bq_bwt.view_department"
# source_id = "ritu-351906.bq_bwt.deprtment0"
# view = bigquery.Table(view_id)
#
# # The source table in this example is created from a CSV file in Google
# # Cloud Storage located at
# # `gs://cloud-samples-data/bigquery/us-states/us-states.csv`. It contains
# # 50 US states, while the view returns only those states with names
# # starting with the letter 'W'.
# view.view_query = f"SELECT name, city FROM `{source_id}` Where city='New Gina'"
#
# # Make an API request to create the view.
# view = client.create_table(view)
# print(f"Created {view.table_type}: {str(view.reference)}")

'''Getting information of data-set'''
# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # TODO(developer): Set dataset_id to the ID of the dataset to fetch.
# dataset_id = 'ritu-351906.bq_bwt'
#
# dataset = client.get_dataset(dataset_id)  # Make an API request.
#
# full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
# friendly_name = dataset.friendly_name
# print(
#     "Got dataset '{}' with friendly_name '{}'.".format(
#         full_dataset_id, friendly_name
#     )
# )
#
# # View dataset properties.
# print("Description: {}".format(dataset.description))
# print("Labels:")
# labels = dataset.labels
# if labels:
#     for label, value in labels.items():
#         print("\t{}: {}".format(label, value))
# else:
#     print("\tDataset has no labels defined.")
#
# # View tables in dataset.
# print("Tables:")
# tables = list(client.list_tables(dataset))  # Make an API request(s).
# if tables:
#     for table in tables:
#         print("\t{}".format(table.table_id))
# else:
#     print("\tThis dataset does not contain any tables.")


# '''to get view information'''
# from google.cloud import bigquery
#
# client = bigquery.Client()
#
# view_id = "ritu-351906.bq_bwt.view_department"
# # Make an API request to get the table resource.
# view = client.get_table(view_id)
#
# # Display view properties
# print(f"Retrieved {view.table_type}: {str(view.reference)}")
# print(f"View Query:\n{view.view_query}")

'''to get table information'''
# from google.cloud import bigquery
#
# client = bigquery.Client()
#
# view_id = "ritu-351906.bq_bwt.deprtment0"
# # Make an API request to get the table resource.
# view = client.get_table(view_id)
#
# # Display view properties
# print(f"Retrieved {view.table_type}: {str(view.reference)}")
# print(f"View Query:\n{view.view_query}")

'''to get dataset list '''
# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# datasets = list(client.list_datasets())  # Make an API request.
# project = client.project
#
# if datasets:
#     print("Datasets in project {}:".format(project))
#     for dataset in datasets:
#         print("\t{}".format(dataset.dataset_id))
# else:
#     print("{} project does not contain any datasets.".format(project))

'Loading JSON data from Cloud Storage'

# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # TODO(developer): Set table_id to the ID of the table to create.
# table_id = "ritu-351906.new_us.new_tab2"
#
# job_config = bigquery.LoadJobConfig(
#     schema=[
#         bigquery.SchemaField("dept_id", "STRING"),
#         bigquery.SchemaField("name", "STRING"),
#         bigquery.SchemaField("salary", "INTEGER"),
#         bigquery.SchemaField("city", "STRING"),
#         bigquery.SchemaField("state", "STRING"),
#         bigquery.SchemaField("country", "STRING"),
#         bigquery.SchemaField("date", "date")
#     ],
#     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
# )
# uri = "gs://cloud_bq_data/csvjson1.json"
#
# load_job = client.load_table_from_uri(
#     uri,
#     table_id,
#     # Must match the destination dataset location.
#     job_config=job_config)  # Make an API request.
#
# load_job.result()  # Waits for the job to complete.
#
# destination_table = client.get_table(table_id)
# print("Loaded {} rows.".format(destination_table.num_rows))

'''replace or append data of table'''
import io

from google.cloud import bigquery

# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # TODO(developer): Set table_id to the ID of the table to create.
# table_id = "ritu-351906.new_us.new_tab2"
#
# job_config = bigquery.LoadJobConfig(
#     schema=[
#         bigquery.SchemaField("dept_id", "STRING"),
#         bigquery.SchemaField("name", "STRING"),
#         bigquery.SchemaField("salary", "INTEGER"),
#         bigquery.SchemaField("city", "STRING"),
#         bigquery.SchemaField("state", "STRING"),
#         bigquery.SchemaField("country", "STRING"),
#         bigquery.SchemaField("date", "date")
#     ],
# )
#
# body = io.BytesIO(b"Washington,WA")
# client.load_table_from_file(body, table_id, job_config=job_config).result()
# previous_rows = client.get_table(table_id).num_rows
# assert previous_rows > 0
#
# job_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
# )
#
# uri = "gs://cloud_bq_data/csvjson1.json"
# load_job = client.load_table_from_uri(
#     uri, table_id, job_config=job_config
# )  # Make an API request.
#
# load_job.result()  # Waits for the job to complete.
#
# destination_table = client.get_table(table_id)
# print("Loaded {} rows.".format(destination_table.num_rows))

'''Running interactive and batch query jobs'''
#
# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# query = """
#     SELECT name
#     FROM `ritu-351906.new_us.new_tab2`
#     LIMIT 20
# """
# query_job = client.query(query)  # Make an API request.
#
# print("The query data:")
# for row in query_job:
#     # Row values can be accessed by field name or index.
#     # print("name={}, count={}".format(row[0], row["total_people"]))
#     print(row)

'''Writing large query results'''

# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client()
#
# # TODO(developer): Set table_id to the ID of the destination table.
# table_id = "ritu-351906.new_us.new_tab22"
#
# job_config = bigquery.QueryJobConfig(destination=table_id)
#
# sql = """
#    SELECT name
#     FROM `ritu-351906.new_us.new_tab2`
#     LIMIT 20
# """

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))
