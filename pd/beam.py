import apache_beam as beam
import argparse


def upper_case(u):
    return u.upper()


def pnt(id):
    return print(id)


def pnt1(id):
    return print(id)
def pnt2(id):
    return print(id)

def f_list(value):

    if value> 5:

     return value

# def int(value):
#     return int(value)
def filter1(x):
    if x[4]=='Male' or x[4]=='Female':
        return x
def domain(x):
    a=''
    b=''
    for i in x:

        if i =='@':
            a+=i
        elif a=='@':
                b+=i

    return b

with beam.Pipeline() as p:
    # combiners use for count the key as per key
    pc1 = (p
           | beam.io.ReadFromText(r'C:\Users\Dell\OneDrive\Desktop\dush1.csv',skip_header_lines=True)
           | beam.Map(lambda x: x.split(','))
           # | beam.Map(lambda x: [x[0],x[1],x[2],x[4],x[5],x[6]])
           # | beam.Filter(lambda x : x[4]=='Male' or x[4]=='Female')
           #  | beam.Filter(filter1)
           # | beam.Map(lambda x: x[3])
           # | beam.Map(domain)
           # | beam.Map(lambda x:(x,1))
           # | beam.combiners.Count.PerKey()
            | beam.Map(lambda x : (x[4],int(x[0])))
           # | beam.Map(print)
           )


# out put like total key and sum of value (a,5) | key is required
#     combiner_Count_PerKey = (pc1
#               | beam.combiners.Count.PerKey()
#               | beam.Map(pnt)
#               )

# out put like total number of rows like ex-5 row in file | key not required
#     combiner_Count_Globally = (pc1
#                 | beam.combiners.Count.Globally()
#                 | beam.Map(pnt2)
#                 )
# apply the aggrigate function on value and apply custome function

    CombinePerKey_sum = (pc1
                               | beam.CombinePerKey(max)  # count , max, min, sum , avg etc
                               | beam.Map(pnt2)
                               )

# out put like (select id,name)- aggrigation
#     join_multiple_feature = (pc1
#                    | beam.Map(lambda x: (x[1][1] + ',' + upper_case(x[1][2])))
#                    | beam.Map(pnt1)
#                    )

# How to create pcollection from memory
    a= [60,45,5,4,4,77,5,7]
    pc_m = (p
            | beam.Create(a)
            # | beam.Map(lambda x : x not in [23,5,4])   # filter by Map return like True and False
            # | beam.Map(print)
            )

# Out put come in filter value in itrable form
#     Filter =( pc_m
#              | beam.Filter(lambda x : x>10)
#              | beam.Map(print)
#               )


# write date into file

    # write = ( Filter
    #          | beam.io.WriteToText('demo.csv')
    #
    # )

# composit Transform by using (beam.PTransform class)
    # use for multiple transform in one code (short the code)
    class My_composit(beam.PTransform):
        def expand(self,input):

         a= (input
                | beam.Filter(lambda list: list[0] < 13)
                    )
         return a



    # filter_id = (pc1
    #              | 'id'>> My_composit()
    #              | beam.Map(pnt))
    #
    # filter_salary = (pc1
    #              |'salary'>> My_composit()
    #              | beam.Map(print))


# ParDo Transformation use for general purpose(can replace map, flatmap, filter transform)

    class Split_row(beam.DoFn):
        def process(self, element):
            if element[1]>2:
               yield element

    # pardo_class = ( pc1
    #                 | beam.ParDo(Split_row())
    #                 | beam.Map(print)
    #              )


#  Side input (passing the additional parameter other than your actual data)
    max_value = 13 # use for side input
    map_value = 1 # use side input
    return_value = (12, 0)
    def max_val(element):

        if element[0] > 13:
            return element
        else:
            return return_value
    #
    # pardo_side_input = (pc1
    #                 | beam.Map(max_val)
    #                 | beam.Map(lambda x : (x[0],x[1],map_value))
    #                 | beam.Map(print)
    #                     )

# ParDo side output


    class filter(beam.DoFn):
        def process(self, element):
            if element[0]==12:
                return [element]

            if element[0]==43:
                return [beam.pvalue.TaggedOutput('aa',element)]

            if element[0]==4:
                return [beam.pvalue.TaggedOutput('cc',element)]

    # pardo_side_output =  ( pc1
    #                        | beam.ParDo(filter()).with_outputs('aa','cc',main='bb')
    #
    # )

    # a_output= pardo_side_output.aa
    # b_output = pardo_side_output.bb
    # c_output = pardo_side_output.cc

    # a_output | beam.Map(print)
    # b_output | beam.Map(pnt)
    # c_output | beam.Map(pnt1)


