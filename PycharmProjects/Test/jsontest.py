#!/usr/bin/python
# -*- coding: utf-8 -*-
import json

data = """

                    "id": 26903,

                    "fieldName": "机会点产品",

                    "contentType": "droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26903"

                },

                {

                    "id": 26904,

                    "fieldName": "新设备型号",

                    "contentType": "droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26904"

                },

                {

                    "id": 26905,

                    "fieldName": "新增设备数量",

                    "contentType": "droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26905"

                },

                {

                    "id": 26906,

                    "fieldName": "新业务类型",

                    "contentType": "droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26906"

                },

                {

                    "id": 26912,

                    "fieldName": "机会点类型",

                    "contentType": "droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26912"

                },

                {

                    "id": 26914,

                    "fieldName": "客户需求",

                    "contentType": "chained_droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26914"

                },

                {

                    "id": 26915,

                    "fieldName": "机会点跟进计划",

                    "contentType": "area_text",

                    "content": "白士业",

                    "fieldCode": "CustomField_26915"

                },

                {

                    "id": 26916,

                    "fieldName": "线下跟进反馈",

                    "contentType": "text",

                    "content": 'null',

                    "fieldCode": "CustomField_26916"

                },

                {

                    "id": 26917,

                    "fieldName": "药事会时间",

                    "contentType": "date",

                    "content": 'null',

                    "fieldCode": "CustomField_26917"

                },

                {

                    "id": 26919,

                    "fieldName": "调整对比剂原因",

                    "contentType": "checkbox",

                    "content": 'null',

                    "fieldCode": "CustomField_26919"

                },

                {

                    "id": 26947,

                    "fieldName": "是否需要通过药事会",

                    "contentType": "chained_droplist",

                    "content": 'null',

                    "fieldCode": "CustomField_26947"

                },

                {

                    "id": 26948,

                    "fieldName": "是否提交申请单",

                    "contentType": "droplist",

                    "content": 1,

                    "fieldCode": "CustomField_26948"

                }
            """

def find_content_by_field_code(json_data, field_code):
    # # 解析 JSON 数据
    # s1 = json.dumps(json_data)
    # data = json.loads(s1)
    # print(data)
    # # 获取 customField 列表
    # custom_fields = data.get("customField", [])
    # # 遍历 customField 列表，查找匹配的 fieldCode
    # for field in custom_fields:
    #     if field.get("fieldCode") == field_code:
    #         return field.get("content")
    # # 如果没有找到匹配的 fieldCode，则返回 None 或其他默认值
    # return None
    for i in json_data:
        print(i)
        if i.get("fieldCode") == field_code:
            return i.get("content")

# 测试示例，传入 fieldCode 并获取对应的 content 值
field_code_to_find = "CustomField_26948"
# content_value = find_content_by_field_code(data, field_code_to_find)

# if content_value is not None:
#     print(f"fieldCode为{field_code_to_find}的content值为: {content_value}")
# else:
#     print(f"未找到fieldCode为{field_code_to_find}的记录。")
import re

# 将 data 转换为适当的格式，即包含多个字典的列表

#
# import re
#
# import json
#
# # 输入的字符串
#
# input_str = "{id: 26903, fieldName: 机会点产品, contentType: droplist, content: 4, fieldCode: CustomField_26903}, {id: 26904, fieldName: 新设备型号, contentType: droplist, content: 7, fieldCode: CustomField_26904}, {id: 26905, fieldName: 新增设备数量, contentType: droplist, content: 1, fieldCode: CustomField_26905}, {id: 26906, fieldName: 新业务类型, contentType: droplist, content: 2, fieldCode: CustomField_26906}, {id: 26912, fieldName: 机会点类型, contentType: droplist, content: 1, fieldCode: CustomField_26912}, {id: 26914, fieldName: 客户需求, contentType: chained_droplist, content: 10, 19, fieldCode: CustomField_26914}, {id: 26915, fieldName: 机会点跟进计划, contentType: area_text, content: 新机器进院 将要开展肝脏特异性检查, fieldCode: CustomField_26915}, {id: 26916, fieldName: 线下跟进反馈, contentType: text, content: None, fieldCode: CustomField_26916}, {id: 26917, fieldName: 药事会时间, contentType: date, content: None, fieldCode: CustomField_26917}, {id: 26919, fieldName: 调整对比剂原因, contentType: checkbox, content: None, fieldCode: CustomField_26919}, {id: 26947, fieldName: 是否需要通过药事会, contentType: chained_droplist, content: None, fieldCode: CustomField_26947}, {id: 26948, fieldName: 是否提交申请单, contentType: droplist, content: 2, fieldCode: CustomField_26948}"
#
# # 使用正则表达式将键和字符串值添加双引号
#
# json_str = re.sub(r'(\w+):\s*([^,}]+)', r'"\1": "\2"', input_str)
#
# try:
#
#     # 使用json.loads将JSON字符串解析为Python数据结构
#
#     data_list = json.loads("[" + json_str + "]")
#
#     # 现在data_list是一个包含字典的列表，您可以按照需要进行操作
#
#     print(str(data_list))
#     for i in data_list:
#         print(i)
#         if i.get("fieldCode") == "CustomField_26948":
#             print(i.get("content"))
# except Exception as e:
#
#     print("解析字符串时出错:", str(e))

input_str = """
{id: 26903, fieldName: 机会点产品, contentType: droplist, content: None, fieldCode: CustomField_26903}, {id: 26904, fieldName: 新设备型号, contentType: droplist, content: None, fieldCode: CustomField_26904}, {id: 26905, fieldName: 新增设备数量, contentType: droplist, content: None, fieldCode: CustomField_26905}, {id: 26906, fieldName: 新业务类型, contentType: droplist, content: None, fieldCode: CustomField_26906}, {id: 26912, fieldName: 机会点类型, contentType: droplist, content: None, fieldCode: CustomField_26912}, {id: 26914, fieldName: 客户需求, contentType: chained_droplist, content: None, fieldCode: CustomField_26914}, {id: 26915, fieldName: 机会点跟进计划, contentType: area_text, content: None, fieldCode: CustomField_26915}, {id: 26916, fieldName: 线下跟进反馈, contentType: text, content: None, fieldCode: CustomField_26916}, {id: 26917, fieldName: 药事会时间, contentType: date, content: None, fieldCode: CustomField_26917}, {id: 26919, fieldName: 调整对比剂原因, contentType: checkbox, content: None, fieldCode: CustomField_26919}, {id: 26947, fieldName: 是否需要通过药事会, contentType: chained_droplist, content: None, fieldCode: CustomField_26947}, {id: 26948, fieldName: 是否提交申请单, contentType: droplist, content: None, fieldCode: CustomField_26948}	{"
{id: 26903, fieldName: 机会点产品, contentType: droplist, content: None, fieldCode: CustomField_26903}, {id: 26904, fieldName: 新设备型号, contentType: droplist, content: None, fieldCode: CustomField_26904}, {id: 26905, fieldName: 新增设备数量, contentType: droplist, content: None, fieldCode: CustomField_26905}, {id: 26906, fieldName: 新业务类型, contentType: droplist, content: None, fieldCode: CustomField_26906}, {id: 26912, fieldName: 机会点类型, contentType: droplist, content: None, fieldCode: CustomField_26912}, {id: 26914, fieldName: 客户需求, contentType: chained_droplist, content: None, fieldCode: CustomField_26914}, {id: 26915, fieldName: 机会点跟进计划, contentType: area_text, content: 白士业, fieldCode: CustomField_26915}, {id: 26916, fieldName: 线下跟进反馈, contentType: text, content: None, fieldCode: CustomField_26916}, {id: 26917, fieldName: 药事会时间, contentType: date, content: None, fieldCode: CustomField_26917}, {id: 26919, fieldName: 调整对比剂原因, contentType: checkbox, content: None, fieldCode: CustomField_26919}, {id: 26947, fieldName: 是否需要通过药事会, contentType: chained_droplist, content: None, fieldCode: CustomField_26947}, {id: 26948, fieldName: 是否提交申请单, contentType: droplist, content: None, fieldCode: CustomField_26948}	{"
{id: 26903, fieldName: 机会点产品, contentType: droplist, content: 4, fieldCode: CustomField_26903}, {id: 26904, fieldName: 新设备型号, contentType: droplist, content: 7, fieldCode: CustomField_26904}, {id: 26905, fieldName: 新增设备数量, contentType: droplist, content: 1, fieldCode: CustomField_26905}, {id: 26906, fieldName: 新业务类型, contentType: droplist, content: 2, fieldCode: CustomField_26906}, {id: 26912, fieldName: 机会点类型, contentType: droplist, content: 1, fieldCode: CustomField_26912}, {id: 26914, fieldName: 客户需求, contentType: chained_droplist, content: 10, 19, fieldCode: CustomField_26914}, {id: 26915, fieldName: 机会点跟进计划, contentType: area_text, content: 新机器进院 将要开展肝脏特异性检查, fieldCode: CustomField_26915}, {id: 26916, fieldName: 线下跟进反馈, contentType: text, content: None, fieldCode: CustomField_26916}, {id: 26917, fieldName: 药事会时间, contentType: date, content: None, fieldCode: CustomField_26917}, {id: 26919, fieldName: 调整对比剂原因, contentType: checkbox, content: None, fieldCode: CustomField_26919}, {id: 26947, fieldName: 是否需要通过药事会, contentType: chained_droplist, content: None, fieldCode: CustomField_26947}, {id: 26948, fieldName: 是否提交申请单, contentType: droplist, content: 2, fieldCode: CustomField_26948}
"""
# 使用正则表达式找到大括号内的内容并拆分为记录列表
record_strings = re.findall(r'\{([^}]+)\}', input_str)
# 初始化结果列表
result_list = []
# 遍历每个记录字符串并转换为字典
for record_string in record_strings:
    # 使用正则表达式分割键值对并创建字典
    record_dict = {}
    key_value_pairs = re.findall(r'(\w+):\s*([^,]+)', record_string)
    for key, value in key_value_pairs:
        record_dict[key.strip()] = value.strip()
    # 添加字典到结果列表
    result_list.append(record_dict)
# 打印结果列表
for record in result_list:
    print(record)
    if record.get("fieldCode") == "CustomField_26948":
            print(record.get("content"))
            