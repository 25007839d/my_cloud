
import apache_beam as beam
from apache_beam.io import ReadFromText,WriteToText

def deco(x):
    print("----------------------")
    print(x)
def deco1(x):

    return print('------------------------''\n',x)
with beam.Pipeline() as pipeline:
    p1 = (pipeline
          | 'read data'>> beam.Create([1,2,3,4])
          |'multiply by 2'>> beam.Map(lambda x:x*2)
          )

    p2 = (p1

          | 'sum value' >> beam.CombineGlobally(sum)
          | 'print'>> beam.Map(deco)

          )
with beam.Pipeline() as pipeline1:


    p3 = ( pipeline1 | "read file">> ReadFromText(r'C:\Users\Dell\Desktop\rdd.txt')
           | "map">> beam.Map(lambda x : x.split(','))

           | 'function' >> beam.Map(deco1)
           | 'write'>> WriteToText(r'C:\Users\Dell\Desktop\rdd1.txt')


    )



