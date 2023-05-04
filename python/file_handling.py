
# file = open(r'C:\Users\Dell\OneDrive\Desktop\PC\dush.txt','w')
#
# # print(file.read(5))
# file.write('hi dushyant here\n')
# file.close()
# f=open(r'C:\Users\Dell\OneDrive\Desktop\PC\dush.txt')
# print(f.read())
import os


import csv
import json
with open(r'C:\Users\Dell\PycharmProjects\cloud\python\data.json', 'r') as f:
    json_obj = json.load(f)

# with open(r'C:\Users\Dell\OneDrive\Desktop\dush1.csv', 'w') as f:
#     wr = csv.DictWriter(f, fieldnames=json_obj[0].keys())
#     wr.writeheader()
#     wr.writerows(json_obj)
#
# with open(r'C:\Users\Dell\OneDrive\Desktop\dush1.csv', 'r') as ff:
#     print(ff.read())
with open(r'C:\Users\Dell\OneDrive\Desktop\ritu-351906-27e10a6678af.json','r')as file:
    conf_obj= json.load(file)

    print(conf_obj)
    for i in conf_obj:
        print(conf_obj[i])

    # for i in conf_obj:
        # print(conf_obj[i])





# import csv
# import json
#
# with open(r'C:\Users\Dell\PycharmProjects\cloud\python\data.json','r') as file:
#     jfile = json.load(file)
#
# with open (r'C:\Users\Dell\PycharmProjects\cloud\python\data1.csv','w') as file1:
#
#     f = csv.DictWriter(file1,fieldnames=jfile[0].keys())
#     f.writeheader()
#     f.writerow(jfile)
# with open(r'C:\Users\Dell\PycharmProjects\cloud\python\data.csv','r')  as o:
#     print(o.read())


