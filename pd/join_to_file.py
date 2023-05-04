import os
import apache_beam as beam
import argparse
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

def select(x):
    print(x)


with beam.Pipeline() as p:
    # combiners use for count the key as per key
    read_data = (p
           | beam.io.ReadFromText(r'C:\Users\Dell\OneDrive\Desktop\PC\f.csv',skip_header_lines=True)
           | beam.Map(lambda x: x.split(','))
           | beam.Map(lambda x:(x[1],x))

           # | beam.Map(print)
           )

    read_data1 = (p
                 | "first file" >> beam.io.ReadFromText(r'C:\Users\Dell\OneDrive\Desktop\PC\f2.csv',skip_header_lines=True)
                 | beam.Map(lambda x: x.split(','))
                 | beam.Map(lambda x: (x[1], x))
                 # | beam.Map(print)
                 )
    key = 'country'
    # join_data_frame =(read_data1,read_data
    #                   | beam.Flatten()
    #                   |beam.Map(print)
    #                   )

    cogroup = ({"read_data":read_data,"read_data1":read_data1}
                | beam.CoGroupByKey()
                | beam.Map(lambda x: x[1]["read_data1"]+ x[1]["read_data"])
                | beam.Map(select)
                | beam.Map(print)
                )
    # format = (cogroup
    #           | 'left join {} and {}'.format(read_data,read_data1)
    #           )


