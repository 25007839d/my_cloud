import os

import apache_beam as beam
import argparse
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions


class split(beam.DoFn):
    def process(self, x):
        yield [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8].split('&'),x[9].split('or'),x[10]]

class explode(beam.DoFn):
    def process(self, x):
        for i in x[8]:
            yield [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],i,x[9],x[10]]

class explode_9(beam.DoFn):
    def process(self, x):
        for i in x[9]:
            yield [x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],i,x[10]]


class l_pad(beam.DoFn):
    def process(self,x):
        if len(x[6]) == 4:
            yield x

        if len(x[6])==3:
            yield  [x[0],x[1],x[2],x[3],x[4],x[5],'0'+x[6],x[7],x[8],x[9],x[10]]

        if len(x[6]) == 2:
            yield [x[0], x[1], x[2], x[3], x[4], x[5],'00' + x[6], x[7], x[8], x[9], x[10]]

        if len(x[6]) == 1:
            yield [x[0], x[1], x[2], x[3], x[4], x[5], '000' + x[6], x[7], x[8], x[9], x[10]]
        if len(x[6]) == 0:
            yield [x[0], x[1], x[2], x[3], x[4], x[5], '0000' + x[6], x[7], x[8], x[9], x[10]]

class filter(beam.DoFn):
    def process(self,element):
        if element[1]=='NJ' and element[2]=='70.E.2.e.BGII(LC)':
            return  [element]

        if element[1]=='NJ' and element[2]=='85.(LC)':
            return [beam.pvalue.TaggedOutput('eighty_five_lc', element)]

        if element[1]=='NJ' and element[2]=='85.TerrMult(LC)':
            return [beam.pvalue.TaggedOutput('eighty_five_terrmult_lc', element)]

        if element[1]=='NJ' and element[2]=='72.E.2.c.(2)(LC)':
            return [beam.pvalue.TaggedOutput('seven_two_e_2_c_lc', element)]

        if element[1]=='NJ' and element[2]=='72.E.2.b.(1)(LC)':
            return [beam.pvalue.TaggedOutput('seven_two_E_2_b_1_lc', element)]

"""
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='<JSON PATH >'
project_id = ""
bucket = ""
table_schema = " "

argv = [
    '--project={0}'.format(project_id),
    '--job_name=gcs_bq_etl',
    '--region=us-central1',
    '--staging_location=gs://{0}/temp/'.format(bucket),
    '--runner=Dataflow'
]
"""
table_schema = 'country:STRING, state:STRING,table_number:STRING,common:STRING,	effective_date:DATE	,expiration_date:DATE,	key1:STRING	,key2:STRING,key3:STRING,key4:INTEGER,	Factor:FLOAT'


with beam.Pipeline() as p:
    # combiners use for count the key as per key
    read_data = (p
           | beam.io.ReadFromText(r'C:\Users\Dell\OneDrive\Desktop\PC\data_file.csv')
           | beam.Map(lambda x: x.split(','))
           # | beam.Map(print)
           )



    split_NJ_state = ( read_data
                       | beam.ParDo(split())
                       # | beam.Map(print)
    )

    explode_split_NJ_state =( split_NJ_state
                            | beam.ParDo(explode())
                            # | beam.Map(print)
    )

    explode_9_split_NJ_state = (explode_split_NJ_state
                              | beam.ParDo(explode_9())
                              # | beam.Map(print)
                              )

    lpad_explode_9_split_NJ_state=(explode_9_split_NJ_state
                                   | beam.ParDo(l_pad())
                                   # | beam.Map(print)
                                   )

    filter_state_table = (lpad_explode_9_split_NJ_state
                          | beam.ParDo(filter()).with_outputs('eighty_five_lc', 'eighty_five_terrmult_lc',
                                                              'seven_two_e_2_c_lc', 'seven_two_E_2_b_1_lc',
                                                              main='seven_two_e_2e_BGII_LC')
                          # |beam.Map(print)
                          )

    eighty_five_lc = filter_state_table.eighty_five_lc
    eighty_five_terrmult_lc = filter_state_table.eighty_five_terrmult_lc
    seven_two_e_2_c_lc = filter_state_table.seven_two_e_2_c_lci
    seven_two_E_2_b_1_lc = filter_state_table.seven_two_E_2_b_1_lc
    seven_two_e_2e_BGII_LC = filter_state_table.seven_two_e_2e_BGII_LC

    eighty_five_lc| beam.Map(print)
    # eighty_five_terrmult_lc | beam.Map(print)

    # write_to_bq_eighty_five_lc = beam.io.WriteToBigQuery(
    #     table="eighty_five_lc",
    #     dataset="dataset",
    #     project="project",
    #     schema=table_schema,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    # )