import apache_beam as beam

import argparse
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions


argparser = argparse.ArgumentParser()

argparser.add_argument('--input',
                        dest="input",
                       default=r'C:\Users\Dell\PycharmProjects\cloud\apachi_beam\data.txt',
                       help='input path'
                       )
argparser.add_argument(
                        '--output',
                            dest='output',
                            default=r'C:\Users\Dell\PycharmProjects\cloud\apachi_beam'
                            ,help='output path'
)

known_ar,pipeline_arg = argparser.parse_known_args() # creat object

pipeline_option = PipelineOptions(pipeline_arg)  # pass argument to pipeline
pipeline_option.view_as(SetupOptions).save_main_session = True  # for parllel and distributed process


class Fdofu(beam.DoFn):
    def process(self,element):
        print(element)
        return '{},{}'.format(element[0],element[1])
def dcs(x):
    print(x)
    return x


with beam.Pipeline(options=pipeline_option) as p:
    line = (p | "read">> beam.io.ReadFromText(known_ar.input)
            | 'split'>> beam.FlatMap(lambda x:x.split())
    | 'map-key'>> beam.Map(lambda x:(x,1))
    | 'reduceperkey'>> beam.CombinePerKey(sum)
    | 'print' >> beam.Map(print)

)

