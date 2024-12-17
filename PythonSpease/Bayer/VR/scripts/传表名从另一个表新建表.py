tables = [
    "analytical_martph_customer360.customer_360_basic_user_info",
    "analytical_martph_customer360.customer_360_basic_kpi_detail",
    "analytical_martph_customer360.customer_360_my_customer",
    "analytical_martph_customer360.customer_360_hco",
    "analytical_martph_customer360.customer_360_opportunity_point",
    "analytical_martph_customer360.customer_360_internal_sales_mtd",
    "analytical_martph_customer360.customer_360_internal_sales_qtd",
    "analytical_martph_customer360.customer_360_internal_sales_ytd",
    "analytical_martph_customer360.customer_360_cda_ct_mr_occupancy",
    "analytical_martph_customer360.customer_360_hospital_equipment",
    "analytical_martph_customer360.customer_360_my_subordinate",
    "analytical_martph_customer360.customer_360_badge",
    "analytical_martph_customer360.customer_360_certificate",
    "analytical_martph_customer360.customer_360_imaging_academy_last_five_reading",
    "analytical_martph_customer360.customer_360_hcp",
    "analytical_martph_customer360.customer_360_last_30days_call_log",
    "analytical_martph_customer360.customer_360_last_three_wechat_visits",
    "analytical_martph_customer360.customer_360_last_three_call",
    "analytical_martph_customer360.customer_360_last_three_meeting",
    "analytical_martph_customer360.customer_360_last_five_behavior",

]

for table in tables:
    schema, table_name = table.split(".")
    print(f"drop table if exists {schema}.{table_name}_prod;")
    print(f"create table {schema}.{table_name}_prod as select * from {schema}.{table_name};")