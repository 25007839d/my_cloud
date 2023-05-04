# import pandas as pd
#
# raw_csv_data = pd.read_csv(r'C:\Users\Dell\OneDrive\Desktop\dush1.csv')
# print(raw_csv_data)
# # for i in raw_csv_data:
# #     print(i)
# df=raw_csv_data.copy()
# print(df.drop(['id'], axis=1))
# print(raw_csv_data['id'].min())
# print(raw_csv_data['id'].max())
# print(raw_csv_data['id'].unique())
# print(len(raw_csv_data['id'].unique()))
# print(sorted(raw_csv_data['id'].unique()) )


l=[1,2,3]
l2=['ram','radha','sita']
d={}
for i in range(len(l)):
    d[l[i]]=l2[i]
print(d)

d2={}
for key in l:
    for value in l2:
        d2[key]=value
        l2.remove(value)
        break
print(d2)