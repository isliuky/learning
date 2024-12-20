tables = [
    "analytical_martph_airepbi.v_report_internal_summary_data",
    "analytical_martph_customer360.customer_360_badge",
    "analytical_martph_customer360.customer_360_basic_kpi_detail",
    "analytical_martph_customer360.customer_360_basic_kpi_detail_his",
    "analytical_martph_customer360.customer_360_basic_user_info",
    "analytical_martph_customer360.customer_360_certificate",
    "analytical_martph_customer360.customer_360_internal_sales_mtd",
    "analytical_martph_customer360.customer_360_internal_sales_mtd_his",
    "analytical_martph_customer360.customer_360_internal_sales_qtd_his",
    "analytical_martph_customer360.customer_360_my_customer_his",
    "analytical_martph_customer360.customer_360_opportunity_point_his",
    "analytical_martph_customer360.customer_360_recruiting_hospital_his",
    "analytical_martph_radsale.report_external_cpa_cha_summary_data",
    "enriched_prestage_airepbi.business_opportunity",
    "enriched_prestage_airepbi.corpinfo",
    "enriched_prestage_radacademy.analysis_learn_log",
    "enriched_prestage_radacademy.analysis_monitor",
    "enriched_prestage_radacademy.bayer_campain_content",
    "enriched_prestage_radacademy.bayer_certificate_member",
    "enriched_prestage_radacademy.bayer_comments",
    "enriched_prestage_radacademy.bayer_content",
    "enriched_prestage_radacademy.bayer_form_question_answer",
    "enriched_prestage_radacademy.bayer_learn_log",
    "enriched_prestage_radacademy.bayer_learn_times_log",
    "enriched_prestage_radacademy.bayer_medal_member",
    "enriched_prestage_radacademy.bayer_meeting_ccmtv_data",
    "enriched_prestage_radacademy.bayer_meeting_ccmtv_data_log",
    "enriched_prestage_radacademy.bayer_member",
    "enriched_prestage_radacademy.bayer_question",
    "enriched_prestage_radacademy.bayer_question_answer",
    "enriched_prestage_radacademy.bayer_score_log",
    "enriched_prestage_radacademy.view_log",
    "enriched_stage_airepbi.airep_province_mapping",
    "enriched_stage_airepbi.business_opportunity",
    "enriched_stage_airepbi.hospital_equipment",
    "enriched_stage_airepbi.internal_ai_rep_hosp_list",
    "enriched_stage_airepbi.userinfo",
    "model_dw_airepbi.dim_user",
    "model_dw_airepbi.dim_user_history",
    "model_dw_airepbi.fact_meeting",
    "model_dw_airepbi.fact_wechat",
    "model_dw_airepbi.scrmwechatbizrecord",
    "model_dw_airepbi.weifeng_mobile",
    "model_dw_radsale.dim_territoryorg_hospital_data",
    "model_dw_radsale.fact_internal_summary_data"
]

for table in tables:
    schema, table_name = table.split(".")
    print(f"-- 比较表: {table}")
    print(f"SELECT * FROM \"cn_cdp_dev\".\"{schema}\".\"{table_name}_bk\"\nEXCEPT\nSELECT * FROM \"cn_cdp_dev\".\"{schema}\".\"{table_name}\";\n")
    print(f"SELECT * FROM \"cn_cdp_dev\".\"{schema}\".\"{table_name}\"\nEXCEPT\nSELECT * FROM \"cn_cdp_dev\".\"{schema}\".\"{table_name}_bk\";\n\n")