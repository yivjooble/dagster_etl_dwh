import datetime
import numpy as np
import scipy.stats
import warnings
import numba
import pandas as pd
from itertools import product
from concurrent.futures import ProcessPoolExecutor
from statsmodels.stats import proportion
from numba import njit, prange,  set_num_threads

from etl_affiliate.utils.utils import exec_query_pg, create_conn

warnings.filterwarnings("ignore")
numba.config.THREADING_LAYER = "default" 
set_num_threads(2)


@njit('float64[:](float64[::1], int_)', parallel=True)
def get_bootstrap_means_numba(x, n_samples: int = 1000) -> np.array:
    """Returns bootstrap replicates of the sample mean based on a given sample.

    Args:
        x (np.array): a sample for bootstrapping.
        n_samples (int): number of the bootstrap replicates of the sample mean to return.
    Returns:
        np.array: an array of the bootstrap replicates of the sample mean of the size n_samples.
    """
    bs_means = np.empty(n_samples, np.float64)
    for s in prange(n_samples):
        bs_sample = np.random.choice(x, size=x.size, replace=True)
        bs_means[s] = np.mean(bs_sample)
    return bs_means


def stat_intervals(stat: np.array, alpha: float) -> np.ndarray:
    """Returns confidence interval calculated on a given sample of bootstrap replicates and significance level.

    Args:
        stat (np.array): a sample of bootstrap replicates to compute CI on.
        alpha (float): significance level.
    Returns:
        np.ndarray: an array with two elements: lower and upper confidence bounds.
    """
    boundaries = np.percentile(stat, [100 * alpha / 2., 100 * (1 - alpha / 2.)])
    return boundaries


def get_bootstrap_statistics(x, n_samples: int = 1000, confidence: float = 0.95):
    """Returns bootstrap statistics based on a given sample (sample mean, LCB & UCB of the CI, the array
        of the bootstrap replicates of the sample mean).

    Args:
        x (np.array): a sample for bootstrapping.
        n_samples (int): number of the bootstrap replicates of the sample mean to return.
        confidence (float): the confidence level for the CI calculation.
    Returns:
        float, float, float, np.array: sample mean, confidence interval LCB, confidence interval UCB, the array
        of the bootstrap replicates of the sample mean.
    """
    x = np.array(x).astype(np.float64)
    mean = np.mean(x)
    bs_means = get_bootstrap_means_numba(x, n_samples=n_samples)
    lower_ci, upper_ci = stat_intervals(bs_means, alpha=1 - confidence)
    return float(mean), float(lower_ci), float(upper_ci), bs_means


def get_delta_intervals(exp_mean: np.array, ctrl_mean: np.array, confidence: float = 0.95) -> tuple:
    """
    Calculate confidence interval for the delta between two means.

    Args:
        exp_mean (np.array): Array of mean values for the experimental group.
        ctrl_mean (np.array): Array of mean values for the control group.
        confidence (float, optional): Confidence level for the intervals (default is 0.95).

    Returns:
        Tuple[float, float]: Lower and upper bounds of the delta confidence interval.

    Note:
        This function assumes that the input arrays `exp_mean` and `ctrl_mean` have the same shape.
    """
    n_zeros_ctrl, n_zeros_exp = (ctrl_mean == 0).sum(), (exp_mean == 0).sum()
    if n_zeros_ctrl == ctrl_mean.shape[0] or n_zeros_exp == exp_mean.shape[0]:
        low_delta, up_delta = 0, 0
        return low_delta, up_delta

    delta_mean = (exp_mean - ctrl_mean) / ctrl_mean
    alpha = 1 - confidence
    low_delta, up_delta = stat_intervals(delta_mean, alpha)
    return low_delta, up_delta


def get_delta_intervals_prop(group_cnt: int, group_obs: int, ctrl_cnt: int, ctrl_obs: int,
                             confidence: float = 0.95) -> tuple:
    """
    Calculate confidence intervals for the delta between two proportions.

    Args:
        group_cnt (int): Count of events in the experimental group.
        group_obs (int): Total observations in the experimental group.
        ctrl_cnt (int): Count of events in the control group.
        ctrl_obs (int): Total observations in the control group.
        confidence (float, optional): Confidence level for the intervals (default is 0.95).

    Returns:
        Tuple[float, float]: Lower and upper bounds of the delta confidence interval.

    Note:
        This function calculates confidence intervals for the difference in proportions between
        the experimental and control groups.
    """
    alpha = 1 - confidence
    if ctrl_obs != 0:
        ctrl_prop = ctrl_cnt / ctrl_obs

        if ctrl_prop != 0:
            lower, upper = proportion.confint_proportions_2indep(group_cnt, group_obs, ctrl_cnt, ctrl_obs, alpha=alpha)
            low_delta = lower / ctrl_prop
            up_delta = upper / ctrl_prop

            return low_delta, up_delta

        return np.nan, np.nan
    return np.nan, np.nan


def get_avg_statistics_vals(df_sample: pd.DataFrame, country_code: str, publisher: str, date_start: str, date_end: str,
                            metric_name: str, test_id: int, iteration: int) -> list:
    """
    Calculate statistical values for A/B test results on average metric.

    Args:
        df_sample (pd.DataFrame): DataFrame containing the test data.
        country_code (str): Country code for filtering the data.
        publisher (str): Publisher name for filtering the data.
        date_start (str): Start date for filtering the data.
        date_end (str): End date for filtering the data.
        metric_name (str): Name of the metric to analyze.
        test_id (int): ID of the A/B test.
        iteration (int): Iteration number for the A/B test.

    Returns:
        List[dict]: List of dictionaries containing aggregated statistical results for each test group and
        specified dimensions.

    Note:
        This function performs statistical analysis on A/B test data and returns a list of dictionaries, where each
        dictionary contains the following measures for each test group compared to the control group:

        - 'country_code': The country code used for filtering.
        - 'publisher': The publisher name used for filtering.
        - 'date_start': The start date used for filtering.
        - 'date_end': The end date used for filtering.
        - 'test_id': The ID of the A/B test.
        - 'iteration': The iteration number for the A/B test.
        - 'test_group': The ID of the test group.
        - 'metric_name': The name of the metric being analyzed.
        - 'metric_value': The mean value of the metric for the test group.
        - 'metric_ci_lower_95': The lower bound of the 95% confidence interval for the metric.
        - 'metric_ci_upper_95': The upper bound of the 95% confidence interval for the metric.
        - 'metric_ci_lower_99': The lower bound of the 99% confidence interval for the metric.
        - 'metric_ci_upper_99': The upper bound of the 99% confidence interval for the metric.
        - 'delta_value': The delta value, i.e., the relative difference between test and control group means.
        - 'delta_ci_lower_95': The lower bound of the 95% confidence interval for the delta.
        - 'delta_ci_upper_95': The upper bound of the 95% confidence interval for the delta.
        - 'delta_ci_lower_99': The lower bound of the 99% confidence interval for the delta.
        - 'delta_ci_upper_99': The upper bound of the 99% confidence interval for the delta.
        - 'u_test_p_value': The p-value from the Mann-Whitney U-test for group comparison.
        - 't_test_p_value': The p-value from the independent two-sample t-test for group comparison.
    """
    df_filtered = filter_data(df_sample, country_code, publisher, date_start, date_end)
    results = []

    control_sample = df_filtered[df_filtered['is_control_group'] == 1][metric_name].dropna().copy()
    ctrl_mean = control_sample.mean()
    _, _, _, ctrl_means = get_bootstrap_statistics(control_sample)

    for test_group in df_filtered['test_group'].unique():
        group_sample = df_filtered[df_filtered['test_group'] == test_group][metric_name].dropna().copy()
        if len(group_sample) > 0:
            metric_value, metric_ci_lower_95, metric_ci_upper_95, bs_means = get_bootstrap_statistics(group_sample)
            _, metric_ci_lower_99, metric_ci_upper_99, _ = get_bootstrap_statistics(group_sample, confidence=0.99)

            if len(control_sample) > 0:
                delta_ci_lower_95, delta_ci_upper_95 = get_delta_intervals(bs_means, ctrl_means)
                delta_ci_lower_99, delta_ci_upper_99 = get_delta_intervals(bs_means, ctrl_means, confidence=0.99)
                if ctrl_mean == 0:
                    delta_value = 0
                else:
                    delta_value = (metric_value - ctrl_mean) / ctrl_mean

                _, u_test_p_value = scipy.stats.mannwhitneyu(group_sample, control_sample, alternative='two-sided')
                _, t_test_p_value = scipy.stats.ttest_ind(group_sample, control_sample, equal_var=False)
            else:
                delta_ci_lower_95, delta_ci_upper_95, delta_ci_lower_99, delta_ci_upper_99 = [np.nan, np.nan, np.nan,
                                                                                              np.nan]
                delta_value, u_test_p_value, t_test_p_value = [np.nan, np.nan, np.nan]

            result = dict(
                country_code=country_code, publisher=publisher, date_start=date_start, date_end=date_end,
                test_id=test_id,
                iteration=iteration, test_group=test_group, metric_name=metric_name,
                metric_value=round(metric_value, 4),
                metric_ci_lower_95=round(metric_ci_lower_95, 4), metric_ci_upper_95=round(metric_ci_upper_95, 4),
                metric_ci_lower_99=round(metric_ci_lower_99, 4), metric_ci_upper_99=round(metric_ci_upper_99, 4),
                delta_value=round(delta_value, 4), delta_ci_lower_95=round(delta_ci_lower_95, 4),
                delta_ci_upper_95=round(delta_ci_upper_95, 4),
                delta_ci_lower_99=round(delta_ci_lower_99, 4), delta_ci_upper_99=round(delta_ci_upper_99, 4),
                u_test_p_value=round(u_test_p_value, 3), t_test_p_value=round(t_test_p_value, 3)
            )
            results.append(result)
    return results


def get_prop_statistics_vals(df_sample: pd.DataFrame, country_code: str, publisher: str, date_start: str, date_end: str,
                             metric_name: str, test_id: int, iteration: int) -> list:
    """
    Calculate statistical values for A/B test results on proportion metric.

    Args:
        df_sample (pd.DataFrame): DataFrame containing the test data.
        country_code (str): Country code for filtering the data.
        publisher (str): Publisher name for filtering the data.
        date_start (str): Start date for filtering the data.
        date_end (str): End date for filtering the data.
        metric_name (str): Name of the metric to analyze.
        test_id (int): ID of the A/B test.
        iteration (int): Iteration number for the A/B test.

    Returns:
        List[dict]: List of dictionaries containing aggregated statistical results for each test group and
        specified dimensions.

    Note:
        This function performs statistical analysis on A/B test data involving proportions and returns a list of
        dictionaries, where each dictionary contains the following measures for each test group compared to the
        control group:

        - 'country_code': The country code used for filtering.
        - 'publisher': The publisher name used for filtering.
        - 'date_start': The start date used for filtering.
        - 'date_end': The end date used for filtering.
        - 'test_id': The ID of the A/B test.
        - 'iteration': The iteration number for the A/B test.
        - 'test_group': The ID of the test group.
        - 'metric_name': The name of the metric being analyzed.
        - 'metric_value': The proportion value of the metric for the test group.
        - 'metric_ci_lower_95': The lower bound of the 95% confidence interval for the metric proportion.
        - 'metric_ci_upper_95': The upper bound of the 95% confidence interval for the metric proportion.
        - 'metric_ci_lower_99': The lower bound of the 99% confidence interval for the metric proportion.
        - 'metric_ci_upper_99': The upper bound of the 99% confidence interval for the metric proportion.
        - 'delta_value': The relative difference in proportions between the test and control groups.
        - 'delta_ci_lower_95': The lower bound of the 95% confidence interval for the delta.
        - 'delta_ci_upper_95': The upper bound of the 95% confidence interval for the delta.
        - 'delta_ci_lower_99': The lower bound of the 99% confidence interval for the delta.
        - 'delta_ci_upper_99': The upper bound of the 99% confidence interval for the delta.
        - 'z_test_p_value': The p-value from the proportions Z-test for group comparison.
        - 'ac_test_p_value': The p-value from the Agresti-Caffo test for group comparison.
    """
    df_filtered = filter_data(df_sample, country_code, publisher, date_start, date_end)
    results = []

    control_sample = df_filtered[(df_filtered['is_control_group'] == 1) & (
            df_filtered['metric_name'] == metric_name)][['metric_numerator', 'metric_denominator']].sum()
    ctrl_obs = control_sample.loc['metric_denominator']
    ctrl_cnt = control_sample.loc['metric_numerator']
    ctrl_prop = ctrl_cnt / ctrl_obs if ctrl_obs != 0 else np.nan

    for test_group in df_filtered['test_group'].unique():
        group_sample = df_filtered[(df_filtered['test_group'] == test_group) & (
                df_filtered['metric_name'] == metric_name)][['metric_numerator', 'metric_denominator']].sum()
        group_obs = group_sample.loc['metric_denominator']
        group_cnt = group_sample.loc['metric_numerator']

        metric_value = group_cnt / group_obs if group_obs != 0 else np.nan
        metric_ci_lower_95, metric_ci_upper_95 = proportion.proportion_confint(group_cnt, group_obs, 0.05)
        metric_ci_lower_99, metric_ci_upper_99 = proportion.proportion_confint(group_cnt, group_obs, 0.01)

        if ctrl_obs == 0 or ctrl_cnt == 0:
            delta_value = 0
        else:
            delta_value = (metric_value / ctrl_prop) - 1

        delta_ci_lower_95, delta_ci_upper_95 = get_delta_intervals_prop(group_cnt, group_obs, ctrl_cnt, ctrl_obs,
                                                                        confidence=0.95)
        delta_ci_lower_99, delta_ci_upper_99 = get_delta_intervals_prop(group_cnt, group_obs, ctrl_cnt, ctrl_obs,
                                                                        confidence=0.99)

        z_test_p_value = proportion.proportions_ztest([group_cnt, ctrl_cnt], [group_obs, ctrl_obs])[1]
        ac_test_p_value = \
            proportion.test_proportions_2indep(group_cnt, group_obs, ctrl_cnt, ctrl_obs, method='agresti-caffo')[1]

        result = dict(
            country_code=country_code, publisher=publisher, date_start=date_start, date_end=date_end, test_id=test_id,
            iteration=iteration, test_group=test_group, metric_name=metric_name, metric_value=round(metric_value, 4),
            metric_ci_lower_95=round(metric_ci_lower_95, 4), metric_ci_upper_95=round(metric_ci_upper_95, 4),
            metric_ci_lower_99=round(metric_ci_lower_99, 4), metric_ci_upper_99=round(metric_ci_upper_99, 4),
            delta_value=round(delta_value, 4), delta_ci_lower_95=round(delta_ci_lower_95, 4),
            delta_ci_upper_95=round(delta_ci_upper_95, 4),
            delta_ci_lower_99=round(delta_ci_lower_99, 4), delta_ci_upper_99=round(delta_ci_upper_99, 4),
            z_test_p_value=round(z_test_p_value, 3), ac_test_p_value=round(ac_test_p_value, 3)
        )
        results.append(result)
    return results


def filter_data(data: pd.DataFrame, country_code: str, publisher: str, date_start: str, date_end: str) -> pd.DataFrame:
    """
    Filter the input DataFrame based on specified dimension values.

    Args:
        data (pd.DataFrame): The DataFrame to be filtered.
        country_code (str): Country code for filtering the data.
        publisher (str): Publisher name for filtering the data.
        date_start (str): Start date for filtering the data.
        date_end (str): End date for filtering the data.

    Returns:
        pd.DataFrame: Filtered DataFrame containing data that matches the specified criteria.

    Note:
        This function filters the input DataFrame based on the provided country code, publisher name,
        and date range (start date to end date).
    """
    country_filter = (data['country_code'] == country_code) if country_code != 'Total' else True
    publisher_filter = (data['publisher'] == publisher) if publisher != 'Total' else True
    date_filter = (data['record_date'].dt.date >= date_start) & (data['record_date'].dt.date <= date_end)
    return data[date_filter & country_filter & publisher_filter]


def get_date_period_list(date_start: str, date_end: str, target_date: datetime.date, return_all: bool = False) -> list:
    """
    Generate a list of date periods between the given start and end dates.

    Args:
        date_start (str): Start date of the period.
        date_end (str): End date of the period.
        target_date (datetime.date): Target date used for generating periods.
        return_all (bool, optional): If True, return all possible date pairs; otherwise, return pairs
                                     for the target date and the day before it (default is False).

    Returns:
        List[Tuple[str, str]]: List of tuples containing date periods.

    Note:
        This function generates a list of date pairs within the specified date range, with each pair
        representing a date period. If return_all is True, it returns all possible date pairs; otherwise,
        it returns pairs for the target date and the day before it.
    """
    period_dates = pd.date_range(date_start, date_end, freq='d')
    day_before_target = target_date - pd.Timedelta(days=1)

    date_periods = []
    for d in period_dates:
        future_dates = [x for x in period_dates if x >= d]
        if return_all:
            date_periods += [(d.date(), y.date()) for y in future_dates]
        else:
            date_periods += [(d.date(), y.date()) for y in future_dates if
                             target_date == y.date() or day_before_target == y.date()]
    return date_periods


def get_dimensions_lists(df: pd.DataFrame, metric_list: list, full_reload_metric_list: list,
                         target_date: datetime.date) -> tuple:
    """
    Generate lists of dimensions for A/B test metric calculations.

    Args:
        df (pd.DataFrame): DataFrame containing the test data.
        metric_list (List[str]): List of metrics to analyze.
        full_reload_metric_list (List[str]): List of metrics requiring full reload (analysis of all possible periods).
        target_date (datetime.date): Target date for metric calculations.

    Returns:
        Tuple[List[str], List[str], List[str], List[str], List[str]]: Lists of country codes, publisher names,
        start dates, end dates, and metric names for metric calculations.

    Note:
        This function generates lists of dimensions based on the provided DataFrame and metric lists.
        It accounts for full reload metrics and target date for calculations.
    """
    not_full_reload_metric_list = [x for x in metric_list if x not in full_reload_metric_list]
    date_periods = get_date_period_list(df['test_min_date'].min(), df['test_max_date'].max(), target_date)
    country_pub_matrix = list(df[['country_code', 'publisher']].drop_duplicates().itertuples(index=False, name=None)) \
        + [(x, 'Total') for x in df['country_code'].unique()] + [('Total', 'Total')]

    dimensions_matrix = list(product(country_pub_matrix, date_periods, not_full_reload_metric_list))

    if len(full_reload_metric_list) > 0:
        date_periods_all = get_date_period_list(df['test_min_date'].min(), df['test_max_date'].max(), target_date,
                                                return_all=True)
        dimensions_matrix += list(product(country_pub_matrix, date_periods_all, full_reload_metric_list))

    country_list = [x[0][0] for x in dimensions_matrix]
    publisher_list = [x[0][1] for x in dimensions_matrix]
    date_start_list = [x[1][0] for x in dimensions_matrix]
    date_end_list = [x[1][1] for x in dimensions_matrix]
    metric_exp_list = [x[2] for x in dimensions_matrix]

    return country_list, publisher_list, date_start_list, date_end_list, metric_exp_list


def get_data_for_calculate_metrics(schema_name: str, func_name: str, full_reload_metric_list: list):
    """
    The function fetches data necessary for calculating A/B testing metrics from a specified DB schema and function. 
    It loads the data in chunks for efficient processing, concatenates these chunks into a single DataFrame, and 
    processes each metric that requires a full reload by deleting existing records from the 
    abtest_significance_metrics table based on test ID, iteration, and metric name.

    Args:
        schema_name (str): Schema name in the database from which data will be fetched.
        func_name (str): The name of the database function for retrieving data.
        full_reload_metric_list (List[str]): List of metrics requiring full reload of the data.

    Returns:
        pandas.DataFrame: Returns a DataFrame containing the data fetched from the specified database function. 
    """
    q = "select * from {func_schema}.{func_name};".format(func_schema=schema_name, func_name=func_name)
    dfs = []

    for chunk in pd.read_sql_query(q, con=create_conn('dwh'), chunksize=5000):
        dfs.append(chunk)

    df = pd.concat(dfs, ignore_index=True)
    df['record_date'] = pd.to_datetime(df['record_date'])

    for metric in full_reload_metric_list:
        for test_id, iteration in df[['test_id', 'iteration']].drop_duplicates().values:
            del_q = '''
                delete from {schema}.abtest_significance_metrics 
                where test_id = {test_id} and iteration = {iteration} and metric_name = \'{metric}\' '''.format(
                schema=schema_name, test_id=test_id, iteration=iteration, metric=metric)
            exec_query_pg(del_q, 'dwh')
    return df


def calculate_ab_test_metrics(schema_name: str, target_date: datetime.date, col_list: list, metric_list: list,
                              full_reload_metric_list: list, df: pd.DataFrame, stat_calc_func=get_avg_statistics_vals):
    """
    The function is designed to calculate A/B testing metrics using parallel computing to process data on different 
    dimensions and iterations. The function aggregates data according to the specified criteria, calls the specified 
    statistical calculation function for each combination of measurements, and writes the results to the database.
    
    Args:
        schema_name (str): Database schema name.
        target_date (datetime.date): Target date for metric calculations.
        col_list (list): A list of columns that will be used for calculations.
        metric_list (List[str]): List of metrics to analyze.
        full_reload_metric_list (List[str]): List of metrics requiring full reload of the data.
        df (pd.DataFrame): DataFrame with the data to be analyzed.
        stat_calc_func  (callable, default is get_avg_statistics_vals): A function for calculating statistical values. 
    
    Notes:
        The function uses the ProcessPoolExecutor to perform calculations in parallel, which improves the 
        speed of processing large amounts of data.
        The results are stored in the abtest_significance_metrics table of the specified scheme in the database.
    """
    for test_id, iteration in df[['test_id', 'iteration']].drop_duplicates().values:
        df_sample = df[(df['test_id'] == test_id) & (df['iteration'] == iteration)][col_list].copy()
        country_list, publisher_list, date_start_list, date_end_list, \
            metric_exp_list = get_dimensions_lists(df_sample, metric_list, full_reload_metric_list, target_date)

        df_sample.drop(['test_min_date', 'test_max_date'], axis=1, inplace=True)

        with ProcessPoolExecutor(max_workers=10) as executor:
            results = executor.map(stat_calc_func, [df_sample] * len(country_list), country_list,
                                   publisher_list, date_start_list, date_end_list, metric_exp_list,
                                   [test_id] * len(country_list),
                                   [iteration] * len(country_list))
        result_list = list(results)
        if len(result_list) > 0:
            df_result = pd.DataFrame.from_records([y for x in result_list for y in x])
            df_result['metric_name'] = df_result['metric_name'].replace(METRIC_NAMES_DICT)
            df_result.replace([np.inf, -np.inf], np.nan, inplace=True)
            # Postgres dwh
            df_result.to_sql(name='abtest_significance_metrics',
                             schema=schema_name,
                             con=create_conn('dwh'),
                             if_exists='append',
                             index=False)
            # Cloudberry dwh
            df_result.to_sql(name='abtest_significance_metrics',
                             schema=schema_name,
                             con=create_conn('cloudberry'),
                             if_exists='append',
                             index=False)


SETTINGS = {
    'job_metrics': {
        'function_name': 'get_abtest_job_metrics_raw({})',
        'column_list': ['publisher', 'country_code', 'record_date', 'is_control_group', 'test_group',
                        'test_min_date', 'test_max_date', 'avg_click_count_per_job', 
                        'avg_certified_click_count_per_job', 'avg_cost_per_job', 'avg_certified_cost_per_job', 
                        'avg_revenue_per_job', 'avg_profit_per_job'],
        'metrics_list': ['avg_click_count_per_job', 'avg_certified_click_count_per_job', 'avg_cost_per_job',
                         'avg_certified_cost_per_job', 'avg_revenue_per_job', 'avg_profit_per_job'],
        'full_reload_metrics_list': [],
        'stat_calc_function': get_avg_statistics_vals
    },
    'click_metrics': {
        'function_name': 'get_abtest_click_metrics_raw({})',
        'column_list': ['publisher', 'country_code', 'record_date', 'is_control_group', 'test_group',
                        'test_min_date', 'test_max_date', 'avg_click_client_cpc', 'avg_certified_click_cpc', 
                        'avg_profit_per_click'],
        'metrics_list': ['avg_click_client_cpc', 'avg_certified_click_cpc', 'avg_profit_per_click'],
        'full_reload_metrics_list': [],
        'stat_calc_function': get_avg_statistics_vals
    },
    'percentage_metrics': {
        'function_name': 'get_abtest_percentage_metrics_raw({})',
        'column_list': ['publisher', 'country_code', 'record_date', 'is_control_group', 'test_group', 'test_id',
                        'iteration', 'metric_name', 'metric_numerator', 'metric_denominator',
                        'test_min_date', 'test_max_date'],
        'metrics_list': ['free click count, %', 'unpaid click count, %', 'ROMI, %', 'conversion rate, %',
                         'conversion rate limited, %'],
        'full_reload_metrics_list': ['conversion rate, %', 'conversion rate limited, %'],
        'stat_calc_function': get_prop_statistics_vals
    }
}


METRIC_NAMES_DICT = {
    'avg_certified_cost_per_job': 'avg certified cost per job, $',
    'avg_click_client_cpc': 'avg click client CPC, $',
    'avg_click_count_per_job': 'avg click count per job',
    'avg_profit_per_click': 'avg profit per click, $',
    'avg_cost_per_job': 'avg cost per job, $',
    'avg_revenue_per_job': 'avg revenue per job, $',
    'avg_certified_click_count_per_job': 'avg certified click count per job',
    'avg_certified_click_cpc': 'avg certified click CPC, $',
    'avg_profit_per_job': 'avg profit per job, $'
}
