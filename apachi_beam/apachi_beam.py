import findspark
findspark.init()
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# class MyOptions(PipelineOptions):
#   @classmethod
#   def _add_argparse_args(cls, parser):
#     parser.add_argument(
#         '--input',
#         default='gs://dush-21/df/data.csv',
#         help='The file path for the input text to process.')
#     parser.add_argument(
#         '--output', required=True, help='The path prefix for output files.')
with beam.Pipeline(P) as pipeline:
    lines = (pipeline | 'ReadMyFile' >> beam.io.ReadFromText(
        r'gs://dush-21/df/data.csv'))

pipeline.run()
