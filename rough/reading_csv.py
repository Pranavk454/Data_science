import os
from pathlib import Path


file = open("C:/Users/prana/Downloads/airtravel.csv",'rb')
string = file.read()
str_data_utf8=str(string,"utf-8") # Changing encoding
list_rows = str_data_utf8.split("\n")
print(list_rows)
print("Length : " , len(list_rows))
print(list_rows[0])
header=None
for elem in list_rows:
    temp=elem.split(",")
    print(temp[0])
    if(temp[0] == "\"Month\""):
        header = elem
        break

print("Printing Header")
data_rows=[]
print(header)

header=header.split(",")

for row in list_rows[1:]:
    obj = {
        'metadata_file_bucket': "One",
        'metadata_file_name': "two"
    }
    row_data = row.split(',')
    if(row_data==['']):
        continue
    print("Row DAta",row_data)

    for idx, item in enumerate(row_data):
        obj[header[idx]] = item
    data_rows.append(obj)

print(data_rows[0][header[0]])

