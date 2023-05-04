#importing library
import apache_beam as beam
from apache_beam.io import ReadFromText,WriteToText
def is_perennial(plant):
  return plant['duration'] == 'perennial'

with beam.Pipeline() as pipeline:
  perennials = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
      | 'Filter perennials' >> beam.Filter(is_perennial)
      | beam.Map(print))

def is_grocerystore(store):
  return store[8] == 'Grocery Store'

#from external resources
# p2 = beam.Pipeline()
#
# grocery = (p2
#            | "Read from Text" >>ReadFromText(r"C:\Users\Dell\PycharmProjects\cloud\apachi_beam\data.txt")
#            | "split the record" >> beam.Map(lambda record: record.split(','))
#            | 'Filter regular' >> beam.Filter(lambda x:x!='is')
#            | 'Write to text'>>beam.io.WriteToText(r'C:\Users\Dell\PycharmProjects\cloud\apachi_beam\dd.txt'))  #| beam.Map(print))
#
#
#
