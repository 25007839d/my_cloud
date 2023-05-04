import collections

import apache_beam as beam
import math

if __name__ == '__main__':

# Reading textfile from local
#     pipe = beam.Pipeline()
#     a = ( pipe
#           |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
#           |beam.Map(lambda x : x.split(","))
#           |beam.Map(print)
#     )
#     pipe.run()

# # Filtering data in file
#     pipe2 = beam.Pipeline()

    # b = ( pipe2
    #      |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
    #      |beam.Map(lambda x : x.split(","))
    #      |beam.Filter(lambda x: x[0]=='USA')
    #      |beam.Map(print)
    # )
    # pipe2.run()
#
# #  custom function
#     def filter_data(element):
#         if element[9]=='':
#             element[9] ='dushyant'
#             return element
#
#         else:
#             return element
#
#     c = ( pipe2
#          |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
#          |beam.Map(lambda x : x.split(","))
#          |beam.Map(filter_data)
#          |beam.Map(print)
#     )
#     pipe2.run()
    '''
filter - do only filter the value with if condition
Map - we can replace or filter
we can use both function

    '''
    def collect(x,cn):
       t =[]
       c= 0
       c = c+x.count()
       print(c)


# gtr# Group By or Aggregation :
# #
    pipe3 = beam.Pipeline()
    ip3 = (pipe3
           |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
           |beam.Map(lambda x: x.split(","))
            # |beam.Map(lambda x:collect(x,3))
            |beam.Filter(lambda row:row.count<10)
           # |beam.Map(lambda x: (x[1],x))
           # |beam.combiners.Count.PerKey()
           |beam.Map(print)
            )
    pipe3.run()



#
#
# # PCollection :
# #
#     # a = [1,2,3,4,5,6,7,8,9,10]
#     # pipe5 = beam.Pipeline()
#     # ip5 = ( pipe5
#     #
#     #         |beam.Create(a)
#     #         |beam.Filter(lambda x : x%2==1)
#     #         |beam.Filter(lambda x : x>4 )
#     #         |beam.Map(print)
#     # )
#     # pipe5.run()
# #
#
# # Combine PerKey:
#
    # pipe6 = beam.Pipeline()
    # ip6 = ( pipe6
    #         |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
    #         |beam.Map(lambda x : x.split(","))
    #         |beam.Map(lambda x : (x[1],x[3]))
    #
    #         |beam.CombinePerKey(sum)
    #         |beam.Map(print)
    #                  )
    # pipe6.run()
# #
#
# # Writings Files :
#
#     # pipe7 = beam.Pipeline()
#     #
#     # ip7 = ( pipe7
#     #         |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
#     #         |beam.Map(lambda x : x.split(","))
#     #         |beam.Map(lambda x : (x[1]+","+x[3],int(x[2])))
#     #         |beam.CombinePerKc(sum)
#     #         |beam.io.WriteToText("beam_output1.txt")
#     # )
#     # pipe7.run()
# #
#
# Composite Transformation : Composite Transforms — Apache Beam allows nested transformations,
# where a transform can perform multiple other transforms, such as ParDo, Combine, etc.
#
    def filter_trans_type(trans,input_elem):
        return input_elem[1] == trans

    def cap1(elem):
        return elem[0],elem[1].title(),elem[2],elem[3]

    def key_vall1(x):
        return (x[3],x[1]+","+(x[2]))

    # pipe8 = beam.Pipeline()
    # ip8 = ( pipe8
    #         |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
    #         |beam.Map(lambda x : x.split(","))
    # )
    #
    # upi = ( ip8
    #         |beam.Filter(lambda x : filter_trans_type("MO",x))
    #         |beam.Map(cap1)
    #         |beam.Map(key_vall1)
    #         |beam.Map(print)
    #         # |beam.io.WriteToText("beam_out_upi1")
    # )
    #
    # cash = (ip8
    #         | beam.Filter(lambda x: filter_trans_type("CO", x))
    #         | 'co'>>beam.Map(cap1)
    #         | 'cash_key_value'>>beam.Map(key_vall1)
    #         |'cash_print'>>beam.Map(print)
    #         # | 'cash_write'>>beam.io.WriteToText("beam_out_cash1")
    # )
    #
    # pipe8.run()
#
#
# #
#
#
#
# # ParDo Transformation :
#     class splitrow(beam.DoFn):
#         def process(self,element):
#             cust = element.split(",")
#             yield cust
#         #or
#             # return [cust]
#         # for Par Do it is must output shoul be in list or iterable
#
#
#     class key_val(beam.DoFn):
#         def process(self,element):
#             a=[element[0]+ element[0],element[1]]
#             return a
#     def filter(element):
#         a=[element[0]+ element[0],element[1]]
#         return a
#     class filter1(beam.DoFn):
#         def process(self,elemnet):
#             if elemnet[1] == "CO":
#                 return [elemnet]
#
#     pipe10 = beam.Pipeline()
#     ip10 = ( pipe10
#              |beam.io.ReadFromText(r"C:\Users\Dell\Desktop\data1.csv",skip_header_lines=1)
#              |beam.ParDo(splitrow())
#              |beam.ParDo(filter1())
#              |beam.ParDo(key_val()) # it return seprate value
#              # |beam.CombinePerKey(sum)
#              |beam.Map(print)
#             )
#     pipe10.run()










# ParDo Side Inputs :
#
#     def max_val(element, max_val):
#         if sum(element)>max_val:
#             return sum(element)
#         else:
#             return 0
#
#     pipe3 = beam.Pipeline()
#     d = ( pipe3 | beam.io.ReadFromText(r"C:\Users\Admin\Downloads\beam_tb1.csv",skip_header_lines=True)
#            | beam.Map(lambda x: x.split(","))
#            | beam.Map(lambda x: (x[3]+","+x[1],int(x[2])))
#            | beam.CombinePerKey(max_val,200)
#            | beam.Map(print)
#     )
#     pipe3.run()


# ParDo Side Output :
    class filter2(beam.DoFn):
        def process(self,element):
            if element[3] == "cash":
                yield element
            if element[3] == "upi":
                yield beam.pvalue.TaggedOutput("upi",element)

    # pipe4 = beam.Pipeline()
    # ip = pipe4 | beam.io.ReadFromText(r"C:\Users\Admin\Downloads\beam_tb1.csv",skip_header_lines=True)
    #
    # out = (ip
    #        | beam.ParDo(splitrow())
    #        | beam.ParDo(filter2()).with_outputs("upi",main="cash")
    # )
    #
    #
    # cash1 = out.cash | beam.io.WriteToText("cashfile3.csv")
    # upi1 = out.upi | beam.io.WriteToText("cashfile3.csv")
    #
    # # cash1 | beam.io.WriteToText("cashfile2.txt")
    # # upi1 | beam.io.WriteToText("upifile2.txt")
    #
    # pipe4.run()








