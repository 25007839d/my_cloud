d = [0]
for i in range (1,6+1):
    d.append(d[i>>1]+int(i & 1))
print(d)