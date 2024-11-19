# import pandas as pd
# from datetime import datetime
# import re
# # 读取Excel文件
# file_path = r'C:\Users\EJOPV\OneDrive - Bayer\Downloads\ddl 1.xlsx'
# df = pd.read_excel(file_path, header=None)
#
# # 初始化变量
# tables = []
# current_table = None
# current_columns = []
#
# # 遍历每一行，解析表名和列信息
# for index, row in df.iterrows():
#     # 检查当前行是否为空
#     if row.isna().all():  # 如果所有元素都是NaN，则跳过此行
#         continue
#
#     # 确保row[0]和row[2]是字符串
#     row_0 = str(row[0])
#     row_2 = str(row[2])
#
#     if row_0.startswith('目标表:'):
#         if current_table:
#             tables.append((current_table, current_columns))
#             current_columns = []
#         current_table = row_0.split(':')[1].strip()
#     elif current_table:
#         column_name = row_0
#         column_type = str(row[1])
#         from_column = row_2.lower()  # 从C列提取from字段并转换为小写
#         if column_type == 'varchar(max)':
#             column_type = 'varchar(65535)'  # Redshift最大varchar长度
#         current_columns.append((column_name, column_type, from_column))
#
# # 添加最后一张表
# if current_table and current_columns:
#     tables.append((current_table, current_columns))
#
# # 生成DDL和INSERT语句
# for table_name, columns in tables:
#     staging_table_name = table_name.split(".")[1]
#     print(staging_table_name)
#     if re.search("cxg", table_name):
#         print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
#         staging_table_name = table_name.split(".")[1]
#         str_to_remove = 'vr_'
#         staging_table_name_result = re.sub(str_to_remove, '', staging_table_name)
#         print(staging_table_name_result)
#     else:
#         print(staging_table_name)
list_test = [
    'analytical_martph_airepbi.v_report_internal_summary_data',
    'analytical_martph_customer360.customer_360_badge',
    'analytical_martph_customer360.customer_360_basic_kpi_detail',
    'analytical_martph_customer360.customer_360_basic_kpi_detail_his',
    'analytical_martph_customer360.customer_360_basic_user_info',
    'analytical_martph_customer360.customer_360_certificate',
    'analytical_martph_customer360.customer_360_internal_sales_mtd',
    'analytical_martph_customer360.customer_360_internal_sales_mtd_his',
    'analytical_martph_customer360.customer_360_internal_sales_qtd_his',
    'analytical_martph_customer360.customer_360_my_customer_his',
    'analytical_martph_customer360.customer_360_opportunity_point_his',
    'analytical_martph_customer360.customer_360_recruiting_hospital_his',
    'analytical_martph_radsale.report_external_cpa_cha_summary_data',
    'enriched_prestage_airepbi.business_opportunity',
    'enriched_prestage_airepbi.corpinfo',
    'enriched_prestage_radacademy.analysis_learn_log',
    'enriched_prestage_radacademy.analysis_monitor',
    'enriched_prestage_radacademy.bayer_campain_content',
    'enriched_prestage_radacademy.bayer_certificate_member',
    'enriched_prestage_radacademy.bayer_comments',
    'enriched_prestage_radacademy.bayer_content',
    'enriched_prestage_radacademy.bayer_form_question_answer',
    'enriched_prestage_radacademy.bayer_learn_log',
    'enriched_prestage_radacademy.bayer_learn_times_log',
    'enriched_prestage_radacademy.bayer_medal_member',
    'enriched_prestage_radacademy.bayer_meeting_ccmtv_data',
    'enriched_prestage_radacademy.bayer_meeting_ccmtv_data_log',
    'enriched_prestage_radacademy.bayer_member',
    'enriched_prestage_radacademy.bayer_question',
    'enriched_prestage_radacademy.bayer_question_answer',
    'enriched_prestage_radacademy.bayer_score_log',
    'enriched_prestage_radacademy.view_log',
    'enriched_stage_airepbi.airep_province_mapping',
    'enriched_stage_airepbi.business_opportunity',
    'enriched_stage_airepbi.hospital_equipment',
    'enriched_stage_airepbi.internal_ai_rep_hosp_list',
    'enriched_stage_airepbi.userinfo',
    'model_dw_airepbi.dim_user',
    'model_dw_airepbi.dim_user_history',
    'model_dw_airepbi.fact_meeting',
    'model_dw_airepbi.fact_meeting',
    'model_dw_airepbi.fact_wechat',
    'model_dw_airepbi.scrmwechatbizrecord',
    'model_dw_airepbi.weifeng_mobile',
    'model_dw_radsale.dim_territoryorg_hospital_data',
    'model_dw_radsale.fact_internal_summary_data',
    'model_dw_radsale.fact_internal_summary_data',
    'analytical_martph_radsale.report_internal_summary_data',
    'enriched_stage_radsale.external_all_hospital_mapping',
    'analytical_martph_airepbi.ai_rep_report_monthly',
    'enriched_stage_airepbi.internal_ai_rep_hosp_list',
    'analytical_martph_airepbi.ai_rep_report_monthly',
    'analytical_martph_airepbi.ai_rep_report_monthly',
]

set_test = set(list_test)
print(set_test)
