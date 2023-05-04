#importing library
import apache_beam as beam

#from external resources
p1 = beam.Pipeline()

grocery = (p1
           | "Read from Text" >> beam.io.ReadFromText(r"../apachi_beam/data.txt", skip_header_lines=1)
           | "split the record" >> beam.Map(lambda record: record.split(','))
           | 'Filter regular' >> beam.Filter(lambda record: record != 'Regular')
            | "print">> beam.Map(print)
           | 'Write to text'>> beam.io.WriteToText('regular_filter.txt'))  #| beam.Map(print))

p1.run()

#In memory
# with beam.Pipeline() as pipeline:
#   lines = (
#       pipeline
#       | beam.Create([
#           'To be, or not to be: that is the question: ',
#           "Whether 'tis nobler in the mind to suffer ",
#           'The slings and arrows of outrageous fortune, ',
#           'Or to take arms against a sea of troubles, ',
#       ]))
#   l2 = (
#       lines | "split">> beam.Map(lambda x: x.split(' '))
#        | "index" >> beam.Map(lambda x: x[2])
#       | "print">> beam.Map(print)
#       |"write">> beam.io.WriteToText('2nd.txt')
#   )
