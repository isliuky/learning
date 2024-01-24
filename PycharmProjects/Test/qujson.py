# 拆分 wefeng qpi 接入的bizfield，成需要的字段 在一行中拼接并返回拆分好的字符串
input_str_data = "{id: 894, content: 1, contentType: text, fieldCode: CustomField_894, name: 台数, optionList: None}, {id: 746, content: 1, contentType: droplist, fieldCode: CustomField_746, name: 主机品牌, optionList: {key: 1, value: GE, subOptionList: }, {key: 2, value: Siemens, subOptionList: }, {key: 3, value: Philips, subOptionList: }, {key: 4, value: Toshiba/Cannon, subOptionList: }, {key: 5, value: 联影, subOptionList: }, {key: 6, value: 东软, subOptionList: }}, {id: 679, content: 1, contentType: droplist, fieldCode: CustomField_679, name: 高压注射器品牌, optionList: {key: 1, value: 拜耳, subOptionList: }, {key: 2, value: 其他品牌, subOptionList: }, {key: 3, value: 无高压注射器, subOptionList: }}, {id: 678, content: 128r, contentType: text, fieldCode: CustomField_678, name: 主机品牌, optionList: None}, {id: 680, content: 1, 4, contentType: chained_droplist, fieldCode: CustomField_680, name: 拜耳高注型号, optionList: {key: 1, value: CT, subOptionList: {key: 4, value: Vistron, subOptionList: }, {key: 5, value: Salient, subOptionList: }, {key: 6, value: Stellant, subOptionList: }}, {key: 2, value: MR, subOptionList: {key: 7, value: MRXP, subOptionList: }, {key: 8, value: SSEP, subOptionList: }}, {key: 3, value: CV, subOptionList: {key: 9, value: Avanta, subOptionList: }, {key: 10, value: Mark 7, subOptionList: }}}, {id: 677, content: 11, contentType: droplist, fieldCode: CustomField_677, name: 主机型号, optionList: {key: 1, value: CT-16排(含大孔径), subOptionList: }, {key: 2, value: CT-64排, subOptionList: }, {key: 3, value: CT-256排, subOptionList: }, {key: 4, value: CT-512排, subOptionList: }, {key: 5, value: CT-其他, subOptionList: }, {key: 6, value: MR-1.5T及以下, subOptionList: }, {key: 7, value: MR-3.0T及以上, subOptionList: }, {key: 8, value: DSA(含各类C臂), subOptionList: }, {key: 9, value: PET-CT, subOptionList: }, {key: 10, value: PET-MR, subOptionList: }, {key: 11, value: CT-128排, subOptionList: }, {key: 12, value: CT- >128排, subOptionList: }, {key: 13, value: CT-32排, subOptionList: }}"
import re
def split_data(input_str_data):
    host_brand_1 = None
    host_model = None
    high_injection_model = None
    high_injection_brand = None
    host_brand_2 = None
    device_num = None
    start_index = 0
    count = input_str_data.count("{id")
    for index in range(count):
        start_dict_index = input_str_data.find("{id:",start_index)
        end_dict_index = input_str_data.find("{id:",start_dict_index+1)
        da_dc = input_str_data[start_dict_index:end_dict_index]
        start_op_index = da_dc.find("{key")
        da_dc_1 = da_dc[:start_op_index].replace("}",'')
        da_dc_2 = da_dc[start_op_index:]
        start_data = re.findall(r'(\w+):\s*([^,]+)', da_dc_1)
        end_data = re.findall(r'(\w+):\s*([^,]+)', da_dc_2)
        print(f'start_data:{start_data}')
        print(f'{end_data}end_data')
        if start_data[3][1] == 'CustomField_894':
            device_num = start_data[1][1]
        if start_data[3][1] == 'CustomField_746':
            host_brand_1 = check_op(start_data,end_data)
        if start_data[3][1] == 'CustomField_679':
            high_injection_brand = check_op(start_data,end_data)
        if start_data[3][1] == 'CustomField_678':
            host_brand_2 = check_op(start_data,end_data)
        if start_data[3][1] == 'CustomField_680':
            high_injection_model = check_op(start_data,end_data)
        if start_data[3][1] == 'CustomField_677':
            host_model = check_op(start_data,end_data)
        start_index = end_dict_index
    result = "{},{},{},{},{},{}".format(device_num,host_brand_1,host_model,high_injection_model,high_injection_brand,host_brand_2)
    return result

def check_op(start_data,end_data):
    field = None
    if start_data[5][1] == 'None':
        field = None
    else:
        for info in end_data:
            if start_data[1][1] == info[1]:
                index = end_data.index(info)
                next_tp = end_data[index + 1][1]
                field = next_tp
    return field

result = split_data(input_str_data)
print(result)
#
# record_strings = re.findall(r'\{([^}]+)\}', input_str)
# result_list = []
# for record_string in record_strings:
#     record_dict = {}
#     key_value_pairs = re.findall(r'(\w+):\s*([^,]+)', record_string)
#     for key, value in key_value_pairs:
#         record_dict[key.strip()] = value.strip()
#     result_list.append(record_dict)
# for record in result_list:
#     print(record)
#     if record.get("fieldCode") == "CustomField_26948":
#         print(record.get("content"))