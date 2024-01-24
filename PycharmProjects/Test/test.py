# # import pandas as pd
# # import xlsxwriter
# # data_csv=pd.read_csv('C://Users//EJOPV//Desktop//enriched_prestage_airepbi.business_opportunity.csv',escapechar="\\",delimiter="|").fillna('null')
# # data_csv.to_excel('C://Users//EJOPV//Desktop//aaa.xlsx',sheet_name='data',engine='xlsxwriter')
# #
# # # import pandas as pd
# # #
# # # df1 = pd.read_excel('C://Users//EJOPV//Desktop//data.xlsx')
# # # df2 = pd.read_excel('C://Users//EJOPV//Desktop//nich_list_1692842361125.xlsx')
# # # comparison_df = df1.equals(df2)
# # # print(df1.head(10))
# # # print(df2.head(10))
# # # if comparison_df:
# # #     print("xiangtong")
# # # else:
# # #     print("butong")
#
# import openpyxl
#
# # 打开Excel文件
# workbook = openpyxl.load_workbook('C://Users//EJOPV//Desktop//nich_list_1692842361125.xlsx')
#
# # 选择工作表
# worksheet = workbook['Sheet1']  # 将'Sheet1'替换为具体的工作表名称
#
# # 读取单元格数据
# data = []
# for row in worksheet.iter_rows(values_only=True):
#     item = {
#         'id': row[0],
#         'fieldName': row[1],
#         'contentType': row[2],
#         'content': row[3],
#         'fieldCode': row[4]
#     }
#     data.append(item)
#
# # 输出格式化的数据
# for item in data:
#     print(item)
from datetime import datetime
from dateutil.relativedelta import relativedelta
# current_date = datetime.now().date()-relativedelta(months=1)
# current_year = current_date.year
# current_month = current_date.month
# print(current_month)
input_value = 202309

import datetime
current_date = datetime.date.today()-relativedelta(months=1)
current_year = current_date.year
current_month = current_date.month

input_year = input_value // 100
input_month = input_value % 100
last_year = current_year - 1
second_last_year = current_year - 2
print(current_year,current_month,input_year,input_month)
if (input_year == current_year and input_month == 9) or (input_year == last_year and input_month == 12) or (input_year == second_last_year and input_month == 12):
        print("test")
else:
    print("sa")