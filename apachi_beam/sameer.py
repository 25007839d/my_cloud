import apache_beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromBigQuery,WriteToBigQuery,BigQueryDisposition


import argparse
import logging

# class fn( apache_beam.DoFn ) :

#     def process(self,elem):
#         return [ elem ]

if _name_ == '_main_':
    logging.getLogger().setLevel( logging.INFO )
    parser = argparse.ArgumentParser()
    parser.add_argument("--input" ,
                        dest = "input"  ,
                        required = True ,
                        help = "Please Provide INPUT GCS PATH" ,
                        type = str  , default = None
                           )

    parser.add_argument( "--output" ,
                         dest = "output" ,
                         required = True ,
                         help = "Please Provide OUTPUT GCS PATH" ,
                         type = str , default =  None  )

    known_args , pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions( pipeline_args )
    pipeline_options.view_as( SetupOptions ).save_main_session= True

    tbl_schema = 'STATE:STRING'
    # QUERY =  """SELECT state FROM `jovial-totality-360004.tar_project.target_table`"""


    with apache_beam.Pipeline(  options = pipeline_options   ) as P :
        BQ_data = ( P
                    | 'Read' >> ReadFromBigQuery(query=known_args.input, use_standard_sql=True ,project = 'jovial-totality-360004'  )
                    # | 'Column' >> beam.Map(column)
                    # | 'format' >> apache_beam.ParDo( fn() )
                    | "write yo bigquery" >> WriteToBigQuery(known_args.output,
                                                            schema=tbl_schema,
                                                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE)
                    )

    from apache_beam.io.gcp.internal.clients import bigquery

    table_spec = bigquery.TableReference(
        projectId='clouddataflow-readonly',
        datasetId='samples',
        tableId='weather_stations')


    table_schema = {
        'fields': [{
            'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'
        }]
    }

    quotes | beam.io.WriteToBigQuery(
        table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)