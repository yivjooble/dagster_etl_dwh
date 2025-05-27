from dagster import job, Field, make_values_resource, fs_io_manager
from datetime import timedelta

from .ops import *
from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.aff_job_config import job_config
from etl_affiliate.utils.utils import job_prefix

JOB_PREFIX = job_prefix()
TARGET_DATE = datetime.today() - timedelta(1)
TARGET_DATE_DIFF = (TARGET_DATE - datetime(1900, 1, 1)).days
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


# Postgres
@job(resource_defs={"globals": make_values_resource(target_date_str=Field(str, default_value=TARGET_DATE_STR),
                                                    target_date_diff=Field(int, default_value=TARGET_DATE_DIFF),
                                                    schema_name=Field(str, default_value='affiliate'),
                                                    send_slack_info=Field(bool, default_value=False),
                                                    check_row_cnt=Field(bool, default_value=True),
                                                    destination_db=Field(str, default_value="dwh")
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     config=job_config,
     name=JOB_PREFIX+'collect_click_statistics',
     description=f'Collects data to the DWH tables of the affiliate schema: temp_bot_click, temp_prod_click, '
                 f'dic_manager, project_cpc_ratio, partner_settings, optimal_cpc_ratio, partner_daily_snapshot,'
                 f'statistics_daily_raw, statistics_daily_agg, prediction_features_v2, external_applies, '
                 f'abtest_stat_agg, fixed_costs. In addition, executes row count check in the basic '
                 f'tables and updates bot clicks (for the day before yesterday) and conversions (for last 30 days) in '
                 f'the statistics_daily_raw and statistics_daily_agg tables.'
     )
def collect_click_statistics_job():
    """Run affiliate data collection flow for the specified date."""
    ps = prc_insert_partner_settings_op()
    check_row_cnt_result = func_get_row_cnt_comparison_op(ps)
    optimal_cpc_ratio_result = prc_insert_optimal_cpc_ratio_op(check_row_cnt_result)
    pcr = prc_insert_project_cpc_ratio_op(optimal_cpc_ratio_result)
    dm = transfer_dic_manager_op(pcr)

    tea_result = transfer_external_applies_op()
    update_conversions_result = prc_update_conversions_op(tea_result)

    collect_temp_bot_click_result = collect_temp_bot_click_op()
    temp_bot_click_dfs = collect_temp_bot_click_result.map(run_temp_bot_click_op).collect()
    temp_bot_click_result = save_temp_bot_click_op(temp_bot_click_dfs)
    collect_temp_prod_click_result = collect_temp_prod_click_op(temp_bot_click_result)
    temp_prod_click_dfs = collect_temp_prod_click_result.map(run_temp_prod_click_op).collect()
    temp_prod_click_result = save_temp_prod_click_op(temp_prod_click_dfs)

    update_bot_result = prc_update_bot_clicks_op(update_conversions_result,
                                                 temp_prod_click_result)

    partner_snapshot_result = prc_insert_partner_daily_snapshot_op(dm, update_bot_result)
    raw_result = prc_insert_statistics_daily_raw_op(partner_snapshot_result)
    # mat_result = prc_v_click_data_op(raw_result)
    agg_result = prc_insert_statistics_daily_agg_op(raw_result)
    billable_closed_job_clicks_costs_result = prc_insert_billable_closed_job_clicks(raw_result)
    fixed_costs_result = prc_insert_fixed_costs(agg_result, billable_closed_job_clicks_costs_result)

    result_tableau = run_tableau_refresh_op(fixed_costs_result)
    prc_insert_prediction_features_v2_op(fixed_costs_result)
    prc_insert_abtest_stat_agg_op(fixed_costs_result)
    refresh_mv_summarized_metrics_op(result_tableau)


# Cloudberry
@job(resource_defs={"globals": make_values_resource(target_date_str=Field(str, default_value=TARGET_DATE_STR),
                                                    target_date_diff=Field(int, default_value=TARGET_DATE_DIFF),
                                                    schema_name=Field(str, default_value='affiliate'),
                                                    send_slack_info=Field(bool, default_value=True),
                                                    check_row_cnt=Field(bool, default_value=True),
                                                    destination_db=Field(str, default_value="cloudberry")
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     config=job_config,
     name=JOB_PREFIX + 'collect_click_statistics_cloudberry',
     description=f'Collects data to the DWH tables of the affiliate schema: temp_bot_click, temp_prod_click, '
                 f'dic_manager, project_cpc_ratio, partner_settings, optimal_cpc_ratio, partner_daily_snapshot,'
                 f'statistics_daily_raw, statistics_daily_agg, prediction_features_v2, external_applies, '
                 f'abtest_stat_agg, fixed_costs. In addition, executes row count check in the basic '
                 f'tables and updates bot clicks (for the day before yesterday) and conversions (for last 30 days) in '
                 f'the statistics_daily_raw and statistics_daily_agg tables.'
     )
def collect_click_statistics_cloudberry_job():
    """Run affiliate data collection flow for the specified date."""
    ps = prc_insert_partner_settings_op()
    check_row_cnt_result = func_get_row_cnt_comparison_op(ps)
    optimal_cpc_ratio_result = prc_insert_optimal_cpc_ratio_op(check_row_cnt_result)
    pcr = prc_insert_project_cpc_ratio_op(optimal_cpc_ratio_result)
    dm = transfer_dic_manager_op(pcr)

    tea_result = transfer_external_applies_op()
    update_conversions_result = prc_update_conversions_op(tea_result)

    collect_temp_bot_click_result = collect_temp_bot_click_op()
    temp_bot_click_dfs = collect_temp_bot_click_result.map(run_temp_bot_click_op).collect()
    temp_bot_click_result = save_temp_bot_click_op(temp_bot_click_dfs)
    collect_temp_prod_click_result = collect_temp_prod_click_op(temp_bot_click_result)
    temp_prod_click_dfs = collect_temp_prod_click_result.map(run_temp_prod_click_op).collect()
    temp_prod_click_result = save_temp_prod_click_op(temp_prod_click_dfs)

    update_bot_result = prc_update_bot_clicks_op(update_conversions_result,
                                                 temp_prod_click_result)

    partner_snapshot_result = prc_insert_partner_daily_snapshot_op(dm, update_bot_result)
    raw_result = prc_insert_statistics_daily_raw_op(partner_snapshot_result)
    mat_result = prc_insert_click_data_op(raw_result)
    agg_result = prc_insert_statistics_daily_agg_op(mat_result)
    billable_closed_job_clicks_costs_result = prc_insert_billable_closed_job_clicks(mat_result)
    fixed_costs_result = prc_insert_fixed_costs(agg_result, billable_closed_job_clicks_costs_result)

    # result_tableau = run_tableau_refresh_op(fixed_costs_result)
    prc_insert_prediction_features_v2_op(fixed_costs_result)
    prc_insert_abtest_stat_agg_op(fixed_costs_result)
    refresh_mv_summarized_metrics_op(fixed_costs_result)
    