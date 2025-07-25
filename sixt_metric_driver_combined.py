from __future__ import annotations
from types import SimpleNamespace
from enum import Enum

import pandas as pd
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout

from ar_analytics import ArUtils
from ar_analytics.defaults import metric_driver_analysis_config, default_table_layout, get_table_layout_vars
from ar_analytics.driver_analysis import DriverAnalysis, DriverAnalysisTemplateParameterSetup
from ar_analytics.helpers.utils import Connector, exit_with_status, NO_LIMIT_N, fmt_sign_num
from ar_analytics.metric_tree import MetricTreeAnalysis
from ar_analytics.breakout_drivers import BreakoutDrivers

import jinja2
import logging
import json

logger = logging.getLogger(__name__)

# SIXT UTILITIES
class SixtTestColumnNames(Enum):
    '''
    Based on the Sixt dataset
    '''

    # Metrics
    CHECKIN_COUNT = "checkin_count"
    DAMAGE_COUNT = "damage_count"
    DAMAGE_DETECTED_AT_CHECKIN_FLG = "damage_detected_at_checkin_flg"
    HIGH_POTENTIALS_COUNT = "high_potentials_count"
    LIVE_CHECKIN_FLG = "live_checkin_flg"
    MONTHS_MATURITY_EMPLOYEE = "months_maturity_employee"
    TARGET_DDR1 = "target_ddr1"
    TARGET_DDR2 = "target_ddr2"
    DAMAGE_AT_CHECK_IN = "damage_at_check_in"
    DDR1 = "ddr1"
    DDR2 = "ddr2"
    LIVE_CHECK_IN_RATE = "live_check_in_rate"
   
    # Grouped Metrics
    DAMAGE_DETECTION_GROUP = "damage_detection"

    # Categorical Dimensions
    BRANCH_TYPE_CONSOLIDATED = "branch_type_consolidated"
    BRNC_NAME = "brnc_name"
    BRNC_POOL = "brnc_pool"
    BRNC_REGION = "brnc_region"
    BRNCH_FIR_NAME = "brnch_fir_name"
    ID_RFP = "id_rfp"
    MANAM_SANITIZED = "manam_sanitized"
    PRODUCT = "product"
    PROFILES_IN_BRANCH = "profiles_in_branch"

    # Example dimension values
    BRANCH_TYPE__AIRPORT = "Airport"
    BRANCH_TYPE__DOWNTOWN = "Downtown"
    BRANCH_TYPE__RAILWAY = "Railway"
    BRNC_REGION__EUROPE = "Europe"
    PRODUCT__PASSENGER_CAR_LONG_TERM = "Passenger Car / Long Term"
    PRODUCT__PASSENGER_CAR_SHORT_TERM = "Passenger Car / Short Term"
    PRODUCT__VAN_TRUCK_LONG_TERM = "Van/Truck / Long Term"
    PRODUCT__VAN_TRUCK_SHORT_TERM = "Van/Truck / Short Term"
    PROFILES__DIFFERENT_ROLES = "DIFFERENT ROLES IN BRANCH"
    PROFILES__ONLY_RSA_ROLES = "ONLY RSA ROLES"

    # Date Dimension
    MAX_TIME_DATE = "max_time_date"
    MONTH = "max_time_month"
    QUARTER = "max_time_quarter"
    YEAR = "max_time_year"

VS_ENABLED_METRICS = [SixtTestColumnNames.DDR1.value, SixtTestColumnNames.DDR2.value]

def check_vs_enabled(metrics):
    if all([metric in VS_ENABLED_METRICS for metric in metrics]):
        return True
    
    return False

# SIXT METRIC DRIVER CLASSES
class SixtMetricTreeAnalysis(MetricTreeAnalysis):
    """
    Metric tree analysis for Sixt
    """
    def __init__(self, sql_exec:Connector=None, df_provider=None, sp=None):
        super().__init__(sql_exec, df_provider, sp)
    
    def run(self, table, metrics, period_filters, query_filters=[], table_specific_filters={}, driver_metrics=[], view="", include_sparklines=True, two_year_filter=None, period_col_granularity='day', metric_props={}, add_impacts=False, impact_formulas={}):
        metric_df = super().run(table, metrics, period_filters, query_filters, table_specific_filters, driver_metrics, view, include_sparklines, two_year_filter, period_col_granularity, metric_props, add_impacts, impact_formulas)
        
        if not check_vs_enabled(metrics):
            return metric_df
        
        additional_filters = table_specific_filters.get('default', [])
        target_metrics = [f"target_{metric}" for metric in metrics]
        target_metrics = [self.helper.get_metric_prop(m, metric_props) for m in target_metrics]
        target_df = self.pull_data_func(metrics=target_metrics, filters=query_filters+additional_filters+[period_filters[0]])

        metric_df['vs Target'] = metric_df.apply(
            lambda row: row['curr'] - target_df[f"target_{row.name}"].iloc[0], 
            axis=1
        )

        return metric_df

class SixtBreakoutDrivers(BreakoutDrivers):
    """
    Breakout drivers for Sixt
    """
    def __init__(self, dim_hierarchy, dim_val_map={}, sql_exec:Connector=None, df_provider=None, sp=None):
        super().__init__(dim_hierarchy, dim_val_map, sql_exec, df_provider, sp)

    def run(self, table, metric, breakouts, period_filters, query_filters=[], table_specific_filters={}, top_n=5, include_sparklines=True, two_year_filter=None, period_col_granularity='day', view="", growth_type="", metric_props={}, dim_props={}):
        breakout_df = super().run(table, metric, breakouts, period_filters, query_filters, table_specific_filters, top_n, include_sparklines, two_year_filter, period_col_granularity, view, growth_type, metric_props, dim_props)
        
        if not check_vs_enabled([metric]):
            return breakout_df
        
        additional_filters = table_specific_filters.get('default', [])
        target_metric = f"target_{metric}"
        target_metric = self.helper.get_metric_prop(target_metric, metric_props)
        dfs = []
        for breakout in breakouts:
            
            target_df = self.pull_data_func(metrics=[target_metric], breakouts=[breakout], filters=query_filters+additional_filters+[period_filters[0]])
            target_df.set_index(breakout, inplace=True)
            target_df.index.name = 'dim_value'
            target_df.index = target_df.index.astype(str)
            dfs.append(target_df)
        target_df = pd.concat(dfs)

        breakout_df['vs Target'] = breakout_df.apply(
            lambda row: row['curr'] - target_df[target_df.index == row.name][f"target_{metric}"].iloc[0], 
            axis=1
        )

        return breakout_df

class SixtMetricDriver(DriverAnalysis):
    """
    Driver for Sixt metric analysis
    """
    def __init__(self, dim_hierarchy, dim_val_map={}, sql_exec: Connector=None, constrained_values={}, compare_date_warning_msg=None, df_provider=None, sp=None):
        super().__init__(dim_hierarchy, dim_val_map, sql_exec, constrained_values, compare_date_warning_msg, df_provider, sp)
        self.mta = SixtMetricTreeAnalysis(sql_exec, df_provider, sp)
        self.ba = SixtBreakoutDrivers(dim_hierarchy, dim_val_map, sql_exec, df_provider, sp)

    def get_display_tables(self, optional_columns=[]):
        metric_df = self._metric_df.copy()
        breakout_df = self._breakout_df.copy()

        # Define required columns for metric_df
        metric_tree_required_columns = ["curr", "prev", "diff", "growth"] + optional_columns
        if self.include_sparklines:
            metric_tree_required_columns.append("sparkline")

        if "impact" in metric_df.columns:
            metric_tree_required_columns.append("impact")

        # Filter metric_df to include only the required columns
        metric_df = metric_df[metric_tree_required_columns]

        # Apply formatting for metric_df
        for col in ["curr", "prev", "diff", "growth"] + optional_columns:
            metric_df[col] = metric_df.apply(
                lambda row: self.helper.get_formatted_num(
                    row[col],
                    self.helper.get_metric_prop(row.name, self.metric_props).get("fmt",
                                                                                 "") if col != "growth" else self.helper.get_metric_prop(
                        row.name, self.metric_props).get("growth_fmt", "")
                ), axis=1
            )

        if "impact" in metric_df.columns:
            metric_df["impact"] = metric_df.apply(
                lambda row: self.helper.get_formatted_num(row["impact"], self.mta.impact_format), axis=1
            )

        # rename columns
        metric_df = metric_df.rename(
            columns={'curr': 'Value', 'prev': 'Prev Value', 'diff': 'Change', 'growth': '% Growth'})
        
        metric_df = metric_df.reset_index()

        # rename index to metric labels
        metric_df["index"] = metric_df["index"].apply(lambda x: self.helper.get_metric_prop(x, self.metric_props).get("label", x))

        # indent non target metric
        metric_df["index"] = metric_df["index"].apply(lambda x: f"  {x}" if x != self.mta.target_metric else x)

        metric_df = metric_df.rename(columns={"index": ""})

        # Define required columns for breakout_df
        breakout_required_columns = ["curr", "prev", "diff", "diff_pct", "rank_change"] + optional_columns
        if self.include_sparklines:
            breakout_required_columns.append("sparkline")

        breakout_dfs = {}

        # Apply formatting for breakout_df
        for col in ["curr", "prev", "diff", "diff_pct"] + optional_columns:
            breakout_df[col] = breakout_df.apply(
                lambda row: self.helper.get_formatted_num(row[col],
                                                          self.ba.target_metric["fmt"] if col != "diff_pct" else
                                                          self.ba.target_metric["growth_fmt"]),
                axis=1
            )

        # Format rank column
        breakout_df["rank_curr"] = breakout_df["rank_curr"]
        breakout_df["rank_change"] = breakout_df.apply(lambda row: f"{int(row['rank_curr'])} ({fmt_sign_num(row['rank_change'])})"
                                                    if (row['rank_change'] and pd.notna(row['rank_change']) and row['rank_change'] != 0)
                                                    else row['rank_curr'], axis=1)
        breakout_df = breakout_df.reset_index()

        breakout_dims = list(breakout_df["dim"].unique())
        if self.ba.dim_hier:
            # display according to the dim hierarchy ordering
            ordering_dict = {value: index for index, value in enumerate(self.ba.dim_hier.get_hierarchy_ordering())}
            # rename cols to dim labels
            ordering_dict = {self.helper.get_dimension_prop(k, self.dim_props).get("label", k): v for k, v in ordering_dict.items()}
            # sort dims by hierarchy order
            breakout_dims.sort(key=lambda x: (ordering_dict.get(x, len(ordering_dict)), x))

        comp_dim = None
        if self.ba._owner_dim:
            comp_dim = next((d for d in breakout_dims if d.lower() == self.ba._owner_dim.lower()), None)

        if comp_dim:
            breakout_dims = [comp_dim] + [x for x in breakout_dims if x != comp_dim]

        for dim in breakout_dims:
            b_df = breakout_df[breakout_df["dim"] == dim]
            if str(dim).lower() == str(comp_dim).lower():
                viz_name = "Benchmark"
            else:
                viz_name = dim
            b_df = b_df.rename(columns={'dim_value': dim})
            b_df = b_df[[dim] + breakout_required_columns]

            # rename columns
            b_df = b_df.rename(
                columns={'curr': 'Value', 'prev': 'Prev Value', 'diff': 'Change', 'diff_pct': '% Growth',
                         'rank_change': 'Rank Change'})
            breakout_dfs[viz_name] = b_df

        return {"viz_metric_df": metric_df, "viz_breakout_dfs": breakout_dfs}

class SixtMetricDriverTemplateParameterSetup(DriverAnalysisTemplateParameterSetup):
    """
    Template parameter setup for Sixt metric driver
    """
    def __init__(self, env=None):
        super().__init__(env=env)
    
    def map_env_values(self, env=None):
        if env is None:
            raise exit_with_status("env namespace is required.")

        driver_analysis_parameters = {}
        pills = {}

        ## Setup DB

        database_id = self.dataset_metadata.get("database_id")
        driver_analysis_parameters["table"] = self.dataset_metadata.get("sql_table")
        driver_analysis_parameters["derived_sql_table"] = self.dataset_metadata.get("derived_table_sql") or ""
        dataset_misc_info = self.dataset_metadata.get("misc_info") or {}
        driver_analysis_parameters["impact_formulas"] = dataset_misc_info.get("impact_formulas") or {}

        driver_analysis_parameters["con"] = Connector("db", database_id=database_id, sql_dialect=self.dataset_metadata.get("sql_dialect"), limit=self.sql_row_limit)

        _, driver_analysis_parameters["dim_hierarchy"] = self.sp.data.get_dimension_hierarchy()
        _, driver_analysis_parameters["driver_metrics"] = self.sp.data.get_metric_hierarchy()
        driver_analysis_parameters["constrained_values"] = self.constrained_values

        ## Map Env Variables

        # Get metric_props, dim_props, setting on env since the chart templates reference these
        env.metric_props = self.get_metric_props()
        env.dim_props = self.get_dimension_props()

        # Get metrics and metric pills

        driver_analysis_parameters["metric"] = env.metric
        metric_pills = self.get_metric_pills(env.metric, env.metric_props)

        # Get filters by dimension
        driver_analysis_parameters["query_filters"], query_filters_pills = self.parse_dimensions(env)

        # Parse breakout dims to the sql columns
        driver_analysis_parameters["breakouts"], breakout_pills = self.parse_breakout_dims(env.breakouts)

        # guardrails for unsupported calculated filters
        calculated_metric_filters = env.calculated_metric_filters if hasattr(env, "calculated_metric_filters") else None
        query, llm_notes, _, _ = self.get_metric_computation_filters([env.metric], calculated_metric_filters, "None", env.metric_props)
        if query:
            self.get_unsupported_filter_message(llm_notes, 'metric drivers')

        ### Period Handling ###

        default_granularity = self.dataset_metadata.get("default_granularity")
        compare_date_warning_msg = None

        if not self.is_period_table:
            start_date, end_date, comp_start_date, comp_end_date = self.handle_periods_and_comparison_periods(env.periods, env.growth_type, allowed_tokens=['<no_period_provided>', '<since_launch>'])

            # date/period column metadata. Assumes the date column is a date type
            period_col = env.date_col if hasattr(env, "date_col") and env.date_col else self.get_period_col()

            if not period_col:
                exit_with_status("A date column must be provided.")

            # create period filters using start date and end date, and comparison start and end dates
            period_filters = []

            if start_date and end_date:
                period_filters.append(
                    { "col": period_col, "op": "BETWEEN", "val": f"'{start_date}' AND '{end_date}'"}
                )

            if comp_start_date and comp_end_date:
                period_filters.append(
                    { "col": period_col, "op": "BETWEEN", "val": f"'{comp_start_date}' AND '{comp_end_date}'" }
                )
                if self.is_date_range_completely_out_of_bounds(comp_start_date, comp_end_date):
                    time_granularity = self.dataset_metadata.get("default_granularity")
                    start_period = self.helper.format_date_from_time_granularity(self.dataset_metadata["min_date"], time_granularity)
                    end_period = self.helper.format_date_from_time_granularity(self.dataset_metadata["max_date"], time_granularity)
                    msg = [f"Please inform the user that the analysis cannot run because data is unavailable for the required {env.growth_type} comparison period."]
                    msg.append(f"Data is only available from {start_period} to {end_period}.")
                    msg.append(f"Ask the user to modify the date range to ensure it aligns with an available {env.growth_type} comparison period within this timeframe.")
                    msg.append("Please do not make any assumptions on behalf of the user.")
                    exit_with_status(" ".join(msg))
                elif self.is_date_range_partially_out_of_bounds(comp_start_date, comp_end_date):
                    compare_date_warning_msg = "Data is only avaiable for partial comparison period. This gap might impact the analysis results and insights."

            two_year_filter = None

            # format dates after adding them to the period filters

            start_date = self.helper.format_date_from_time_granularity(start_date, default_granularity)
            end_date = self.helper.format_date_from_time_granularity(end_date, default_granularity)
            comp_start_date = self.helper.format_date_from_time_granularity(comp_start_date, default_granularity)
            comp_end_date = self.helper.format_date_from_time_granularity(comp_end_date, default_granularity)

            date_labels = {"start_date": start_date, "end_date": end_date, "compare_start_date": comp_start_date, "compare_end_date": comp_end_date}

        else:
            _, periods_in_year = self.sp.data.get_periods_in_year()
            date_filters, _ = self.get_time_variables(env.periods)
            period_filters, date_labels = self.get_period_filters(sql_con=driver_analysis_parameters["con"],
                                                        date_filters=date_filters,
                                                        growth_type=env.growth_type,
                                                        sparkline_n_year=2)

            if period_filters:
                two_year_filter = period_filters[-1]
                period_filters = period_filters[:-1]
            else:
                two_year_filter = None

            print("period_filters")
            print(period_filters)

            if str(env.growth_type).lower() != "none" and not date_labels.get("compare_start_date"):
                msg = ["Please inform the user that the analysis cannot run because data is unavailable for the required year-over-year (Y/Y) comparison period."]
                msg.append(f"Data is only available from {date_labels.get('data_start_date')} to {date_labels.get('data_end_date')}.")
                msg.append(f"Ask the user to modify the date range to ensure it aligns with an available {env.growth_type} comparison period within this timeframe.")
                msg.append("Please do not make any assumptions on behalf of the user.")
                exit_with_status(" ".join(msg))
            elif self.is_period_date_partially_out_of_bounds(period_filters):
                compare_date_warning_msg = "Data is only avaiable for partial comparison period. This gap might impact the analysis results and insights."

            start_date = date_labels.get("start_date")
            end_date = date_labels.get("end_date")
            comp_start_date = date_labels.get("compare_start_date")
            comp_end_date = date_labels.get("compare_end_date")

        # Set the trend date parameters
        driver_analysis_parameters["date_labels"] = date_labels
        driver_analysis_parameters["period_filters"] = period_filters
        driver_analysis_parameters["period_col_granularity"] = "day"
        driver_analysis_parameters["two_year_filter"] = two_year_filter
        driver_analysis_parameters["compare_date_warning_msg"] = compare_date_warning_msg

        # convert limit_n to an int
        if hasattr(env, "limit_n") and env.limit_n:
            if env.limit_n == NO_LIMIT_N:
                driver_analysis_parameters["limit_n"] = None
            else:
                driver_analysis_parameters["limit_n"] = self.convert_to_int(env.limit_n)

        # set growth type

        driver_analysis_parameters["growth_type"] = env.growth_type

        # use sparklines

        env.include_sparklines = True # must be set since the chart references env.include_sparklines
        driver_analysis_parameters["include_sparklines"] = env.include_sparklines

        ## add UI bubbles

        if metric_pills:
            pills["metric"] = f"Metric: {self.helper.and_comma_join(metric_pills)}"
        if query_filters_pills:
            pills["filters"] = f"Filter: {self.helper.and_comma_join(query_filters_pills)}"
        if breakout_pills:
            pills["breakout"] = f"Breakout: {self.helper.and_comma_join(breakout_pills)}"
        if start_date and end_date:
            if start_date == end_date:
                pills["period"] = f"Period: {start_date}"
            else:
                pills["period"] = f"Period: {start_date} to {end_date}"
        # Only show compare period if not using target comparison
        if comp_start_date and comp_end_date and not check_vs_enabled([env.metric]):
            if comp_start_date == comp_end_date:
                pills["compare_period"] = f"Compare Period: {comp_start_date}"
            else:
                pills["compare_period"] = f"Compare Period: {comp_start_date} to {comp_end_date}"
        elif check_vs_enabled([env.metric]):
            pills["comparison"] = "vs Target"
        if hasattr(env, "growth_type"):
            if str(env.growth_type).lower() in ["p/p", "y/y"]:
                pills["growth_type"] = f"Growth Type: {str(env.growth_type)}"

        driver_analysis_parameters["ParameterDisplayDescription"] = pills

        ## Set the driver analysis parameters
        env.driver_analysis_parameters = driver_analysis_parameters

# MAIN SKILL
@skill(
    name=metric_driver_analysis_config.name,
    llm_name=metric_driver_analysis_config.llm_name,
    description=metric_driver_analysis_config.description,
    capabilities=metric_driver_analysis_config.capabilities,
    limitations=metric_driver_analysis_config.limitations,
    example_questions="Show me advanced driver analysis for sales vs budget by quarter",
    parameter_guidance=metric_driver_analysis_config.parameter_guidance,
    parameters=[
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods. If given a range, for example 'last 3 quarters, 'between q3 2022 to q4 2023' etc, enumerate the range into a list of valid dates. Don't include natural language words or phrases, only valid dates like 'q3 2023', '2022', 'mar 2020', 'ytd sep 2021', 'mat q4 2021', 'ytd q1 2022', 'ytd 2021', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>' etc."
        ),
        SkillParameter(
            name="metric",
            is_multi=False,
            constrained_to="metrics"
        ),
        SkillParameter(
            name="metric_group",
            is_multi=False,
            constrained_to="metric_groups",
            description="Metric group used to pull grouped metrics"
        ),
        SkillParameter(
            name="limit_n",
            description="limit the number of values by this number",
            default_value=10
        ),
        SkillParameter(
            name="breakouts",
            is_multi=True,
            constrained_to="dimensions",
            description="breakout dimension(s) for analysis."
        ),
        SkillParameter(
            name="growth_type",
            constrained_to=None,
            constrained_values=["Y/Y", "P/P"],
            description="Growth type either Y/Y or P/P",
            default_value="Y/Y"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="calculated_metric_filters",
            description='This parameter allows filtering based on computed values like growth, delta, or share. The computed values are only available for metrics selected for this analysis. The available computations are growth, delta and share. It accepts a list of conditions, where each condition is a dictionary with:  metric: The metric being filtered. computation: The computation (growth, delta, share) operator: The comparison operator (">", "<", ">=", "<=", "between", "=="). value: The numeric threshold for filtering. If using "between", provide a list [min, max]. scale: the scale of value (percentage, bps, absolute)'
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=metric_driver_analysis_config.max_prompt
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=metric_driver_analysis_config.insight_prompt
        ),
        SkillParameter(
            name="table_viz_layout",
            parameter_type="visualization",
            description="Table Viz Layout",
            default_value=default_table_layout
        )
    ]
)
def simple_metric_driver(parameters: SkillInput):
    param_dict = {"periods": [], "metric": "", "metric_group": "", "limit_n": 10, "breakouts": None, "growth_type": "Y/Y", "other_filters": [], "calculated_metric_filters": None}
    print(f"Skill received following parameters: {parameters.arguments}")
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    env = SimpleNamespace(**param_dict)
    SixtMetricDriverTemplateParameterSetup(env=env)
    env.da = SixtMetricDriver.from_env(env=env)

    _ = env.da.run_from_env()

    optional_columns = ["vs Target"] if check_vs_enabled([env.metric]) else []
    results = env.da.get_display_tables(optional_columns=optional_columns)

    tables = {
        "Metrics": results['viz_metric_df']
    }
    tables.update(results['viz_breakout_dfs'])

    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.da.paramater_display_infomation.items()]

    insights_dfs = [env.da.df_notes, env.da.breakout_facts, env.da.subject_fact.get("df", pd.DataFrame())]

    warning_messages = env.da.get_warning_messages()

    viz, insights, final_prompt, export_data = render_layout(tables,
                                                            env.da.title,
                                                            env.da.subtitle,
                                                            insights_dfs,
                                                            warning_messages,
                                                            parameters.arguments.max_prompt,
                                                            parameters.arguments.insight_prompt,
                                                            parameters.arguments.table_viz_layout)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[],
        export_data=[ExportData(name=name, data=df) for name, df in export_data.items()]
    )

def render_layout(tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt, viz_layout):
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)
    viz_list = []
    export_data = {}

    general_vars = {"headline": title if title else "Total",
                    "sub_headline": subtitle if subtitle else "Driver Analysis",
                    "hide_growth_warning": False if warnings else True,
                    "exec_summary": insights if insights else "No Insights.",
                    "warning": warnings}

    for name, table in tables.items():
        export_data[name] = table
        hide_footer = True
        table_vars = get_table_layout_vars(table, sparkline_col="sparkline")
        table_vars["hide_footer"] = hide_footer
        rendered = wire_layout(json.loads(viz_layout), {**general_vars, **table_vars})
        viz_list.append(SkillVisualization(title=name, layout=rendered))

    return viz_list, insights, max_response_prompt, export_data

if __name__ == '__main__':
    skill_input: SkillInput = simple_metric_driver.create_input(
        arguments={
  "breakouts": [
    "brand"
  ],
  "metric": "sales",
  "periods": [
    "2022",
    "2023"
  ]
})
    out = simple_metric_driver(skill_input)
    preview_skill(simple_metric_driver, out)