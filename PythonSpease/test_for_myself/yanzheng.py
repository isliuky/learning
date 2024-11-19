# import json
# import pandas as pd
#
# json_file = "C://Users//EJOPV//Desktop//response.json"
#
# with open(json_file,'r',encoding="utf8") as json_data_file:
#     json_data = json.load(json_data_file)
# json_data = json_data.get("data",[])
# data = pd.DataFrame(json_data)
#
# csv_file =  "C://Users//EJOPV//Desktop//enriched_prestage_airepbi.business_opportunity.csv"
#
# csv_data = pd.read_csv(csv_file,encoding='utf-8', sep='|')
# for i,a in csv_data.iterrows():
#     print(i)
#     print(a)
#     print(data[i])
# # for index,cscrow in csv_data.iterrows():
# #     json_row = json_data[index] if index < len(json_data) else None
# #     print(csv_data.to_dict())
# #     print(json_row)
# #     print("=============")
# #     if csv_data.to_dict() == json_row:
# #         print("一致")
# #     else:
# #         print("不一致")
import csv
import jsontest

csv_file =  "C://Users//EJOPV//Desktop//enriched_prestage_airepbi.business_opportunity.csv"
with open(csv_file,mode='r',encoding='utf-8')as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter= '|')
    json_file = "C://Users//EJOPV//Desktop//response.json"
    with open(json_file,'r',encoding='utf-8')as json_data_file:
        json_data = json.load(json_data_file).get("data",[])
    row_number = 0
    for csv_row,json_row in zip(csv_reader,json_data):
        row_number += 1
        for key,value in csv_row.items():
            if value == '':
                csv_row[key] = None
        print(csv_row)
        print(json_row)
        if csv_row == json_row:
            print("yizhi")
        else:
            print("bu")