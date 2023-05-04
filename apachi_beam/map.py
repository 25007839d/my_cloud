import apache_beam as beam
def strip_header_and_newline(text):
  return text.strip('# \n')

#Map
#• Applies a simple 1-to-1 mapping function over each element in the collection.
with beam.Pipeline() as pipeline:
  plants = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          '# 🍓Strawberry\n',
          '# 🥕Carrot\n',
          '# 🍆Eggplant\n',
          '# 🍅Tomato\n',
          '# 🥔Potato\n',
      ])
      | 'Strip header' >> beam.Map(strip_header_and_newline)
      | beam.Map(print))

# MapTuple for key-value pairs
#MapTuple for key-value pairs. If your PCollection consists of (key, value) pairs,
# you can use MapTuple to unpack them into different function arguments.
with beam.Pipeline() as pipeline:
  plants = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          ('🍓', 'Strawberry'),
          ('🥕', 'Carrot'),
          ('🍆', 'Eggplant'),
          ('🍅', 'Tomato'),
          ('🥔', 'Potato'),
      ])
      | 'Format' >> beam.MapTuple(lambda icon, plant: '{}{}'.format(icon, plant))
      | beam.Map(print))


#FlatMap
#• Applies a simple 1-to-many mapping function over each element in the collection. The many elements are
# flattened into the resulting collection.
def split_words(text):
  return text.split(',')

with beam.Pipeline() as pipeline:
  plants = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          '🍓Strawberry,🥕Carrot,🍆Eggplant',
          '🍅Tomato,🥔Potato',
      ])
      | 'Split words' >> beam.FlatMap(split_words)
      | beam.Map(print))


#FlatMapTuple for key-value pairs

#If your PCollection consists of (key, value) pairs, you can use FlatMapTuple to unpack them into
# different function arguments.
def format_plant(icon, plant):
  if icon:
    yield '{}{}'.format(icon, plant)



with beam.Pipeline() as pipeline:
  plants = (
      pipeline
      | 'Gardening plants' >> beam.Create([
          ('🍓', 'Strawberry'),
          ('🥕', 'Carrot'),
          ('🍆', 'Eggplant'),
          ('🍅', 'Tomato'),
          ('🥔', 'Potato'),
          (None, 'Invalid'),
      ])
      | 'Format' >> beam.FlatMapTuple(format_plant)
      | beam.Map(print))

#Filter
#Given a predicate, filter out all elements that don’t satisfy that predicate.
# May also be used to filter based on an inequality with a given value based on the comparison ordering of the element.

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
