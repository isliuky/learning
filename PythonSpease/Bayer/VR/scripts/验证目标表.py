import re


def modify_tables_and_schema(file_path, output_path):
    # 第一步：正则表达式模式用于匹配 cn_cdp_dev_vr_2 模式下的表名并在其后添加'prod'
    table_pattern = re.compile(r'("cn_cdp_dev_vr_2"\."analytical_martph_customer360")\."([^"]+)"', re.IGNORECASE)

    # 第二步：正则表达式模式用于全局替换 cn_cdp_dev_vr_2 为 cn_cdp_dev
    schema_pattern = re.compile(r'\b(cn_cdp_dev_vr_2)\b', re.IGNORECASE)

    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    def add_prod(match):
        # 获取匹配的模式和表名部分并在表名后添加'prod'
        schema_part = match.group(1)
        table_name = match.group(2)
        return f'{schema_part}."{table_name}prod"'

    # 使用正则表达式的sub方法替换匹配到的内容（表名加prod）
    modified_sql = table_pattern.sub(add_prod, sql_content)

    # 全局替换 cn_cdp_dev_vr_2 为 cn_cdp_dev
    final_sql = schema_pattern.sub('cn_cdp_dev', modified_sql)

    with open(output_path, 'w', encoding='utf-8') as file:
        file.write(final_sql)

# 调用函数，指定输入和输出文件路径
add_prod_to_tables(r'C:\Users\EJOPV\OneDrive - Bayer\Desktop\input.sql', r'C:\Users\EJOPV\OneDrive - Bayer\Desktop\output.sql')