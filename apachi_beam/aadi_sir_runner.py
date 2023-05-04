import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions
from apache_beam.io import ReadFromText,WriteToText

if __name__=="--main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default=r'C:\Users\Dell\PycharmProjects\cloud\apachi_beam\data.txt',
                        required = True)
    parser.add_argument('--output',
                        dest='output',
                        default='  path',
                        required = True)
    know_arg,pipeline_arg = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_arg)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    def test (element):
        print('######################')
        print(element)

    with beam.Pipeline(options=pipeline_options) as p:
        line = (p | 'read'>>ReadFromText(r'C:\Users\Dell\PycharmProjects\cloud\apachi_beam\data.txt'))

        sunval = (line |
                  'split'>> beam.FlatMap(lambda x:x.split(','))
                  | "pairwithone">>beam.Map(lambda x : x,1)
                  | 'valueuse'>> beam.CombinePerKey(sum)
                  | "test">> beam.Map(test))
    p.run()
# python3 file.py--input gs:// --output gs:// --runner DataflowRunner
# --runner DataflowRunner --project --temp_location gs://ritu_bucket/temp/ --staging_location gs://ritu_bucket/temp/ --region us-central1
