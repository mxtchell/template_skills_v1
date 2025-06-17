import copy
from collections import defaultdict
from datetime import datetime

import re

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

# grap all the relevant helper imports
from ar_analytics.helpers.utils import OldDimensionHierarchy, SharedFn, Connector, SkillPlatform, \
    exit_with_status, old_split_dim_and_metric_filters, old_get_date_label_str, is_using_max_sql_gen, old_get_filters_headline, TemplateParameterSetup, \
    NO_LIMIT_N, get_viz_header, is_filter_token, pull_data

from ar_analytics.ar_utils import ArUtils

# Do not remove, pulls in max_metadata on all pandas DFs
import answer_rocket

# special tokens
ALL = "<all>"
FASTEST_GROWING = "<fastest growing>"
HIGHEST_GROWING = "<highest growing>"
FASTEST_DECLINING = "<fastest declining>"
HIGHEST_DECLINING = "<highest declining>"
BIGGEST = "<biggest overall>"
SMALLEST = "<smallest overall>"
OTHER = "<other>"
COMPETITORS = "<competitors>"
DATE_SEQ_COL_SUFFIX = "_sequence"

GROWTH = "_growth"
DELTA = "_delta"
VARIANCE_DELTA = "_variance_delta"
VARIANCE_GROWTH = "_variance_growth"
VARIANCE_TYPES = ['vs. budget', 'vs. target', 'vs. forecast'] # TODO: Figure out how to pass variance as a parameter, currently using the growth_type parameter

# jinja template for chart
NON_GROWTH_CHART_TEMPLATE = """
{% set chart_name = df["chart_name"].iloc[0] %}
{% set chart_obj = trend.display_charts[chart_name] %}
{
    "chart": {
        "type": "line"
    },
    "title": {
        "text": "{{ name }}",
        "align": "center",
        "style": {
            "fontSize": "14px"
        }
    },
    "xAxis": {
        "categories": {{ df[trend.date_alias].to_list() | tojson }},
        "title": {
            "enabled": false
        }
    },
    "yAxis": [
        {% for axis in chart_obj["yaxis"] %}
        {
            "title": {
                "text": "{{ axis['title'] }}"
            },
            "labels": {
                "format": "{{trend.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')}}"
            },
            "opposite": {{'true' if axis['opposite'] else 'false'}}
        }
        {% if not loop.last %},{% endif %}
        {% endfor %}
    ],

    "series": [
    {% set variable_cols = [] %}
    {% for col in df.columns %}
        {% if col not in ['date_column', trend.date_alias, 'month', 'metric', 'fmt', 'chart_name'] %}
           {% set variable_cols = variable_cols.append(col) %}
        {% endif %}
    {% endfor %}
    {% for col in variable_cols %}
        {% if col not in ['date_column', trend.date_alias, 'chart_name', 'fmt'] %}
            {% if not chart_obj["dim_breakout"] %}
                {% set line_name = chart_obj['name'] if chart_obj['name'] else sfn.get_metric_prop(col, metric_props)['label'] %}
                {% set num_format = chart_obj['fmt'] if chart_obj['fmt'] else sfn.get_metric_prop(col, metric_props)['fmt'] %}
            {% else %}
                {% set num_format = chart_obj['fmt'] if chart_obj['fmt'] else sfn.get_metric_prop(col, metric_props)['fmt'] %}
                {% set line_name = chart_obj['name'] if chart_obj['name'] else col %}
            {% endif %}
            {
                "name": "{{ line_name }}",
                "data": {{ sfn.replace_nans_with_string_nan(df[col].to_list()) | tojson }},
                {% if not chart_obj["dim_breakout"] %}
                    "yAxis": {{chart_obj["yaxis_index"][col]}},
                {% else %}
                    "yAxis": 0,
                {% endif %}
                "tooltip": {
                    "pointFormat": "<span style='color:{point.color}'>\u25CF</span> {{line_name}}: {{trend.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')}}"
                },
                "dataLabels": {
                        "enabled": {{'true' if trend.show_labels else 'false'}},
                        "align": "center",
                        "verticalAlign": "top",
                        "y": -20,
                        "format": "{{trend.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')}}"
                },
                "pointPadding": 0
            }
            {% if not loop.last %},{% endif %}
        {% endif %}
    {% endfor %}
    ],
    "credits": {
        "enabled": false
    }
}
"""

CHART_TEMPLATE = """
{% set chart_name = df["chart_name"].iloc[0] %}
{% set chart_obj = trend.display_charts[chart_name] %}
{
    "chart": {
        "type": "line"
    },
    "title": {
        "text": "{{ name }}",
        "align": "center",
        "style": {
            "fontSize": "14px"
        }
    },
    "xAxis": {
        "categories": {{ chart_obj['xAxis'] | tojson }},
        "title": {
            "enabled": false
        }
    },
    "yAxis": [
        {% for axis in chart_obj['axis_info'][axis_type]["yaxis"] %}
        {
            "title": {
                "text": "{{ axis['title'] }}"
            },
            "labels": {
               "format": "{{trend.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')}}"
            },
            "opposite": {{'true' if axis['opposite'] else 'false'}}
        }
        {% if not loop.last %},{% endif %}
        {% endfor %}
    ],

    "series": [
    {% if not chart_obj["dim_breakout"] %}
        {% for col in df.columns %}
            {% if col not in ['date_column', trend.date_alias, 'chart_name'] %}
                {% set line_name = chart_obj['name'] if chart_obj['name'] else sfn.get_metric_prop(col, metric_props)['label'] %}
                {% set num_format = chart_obj['fmt'] if chart_obj['fmt'] else sfn.get_metric_prop(col, metric_props)['fmt'] %}
                {
                                "name": "{{ line_name }}",
                                "id": "{{ line_name }}",
                                "data": {{ sfn.replace_nans_with_string_nan(df[col].to_list()) | tojson }},
                                "yAxis": {{chart_obj["yaxis_index"][col]}},
                                "tooltip": {
                                    "pointFormat": "<span style='color:{point.color}'>\u25CF</span> {{line_name}}: {{trend.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')}}"
                                },
                                "dataLabels": {
                                        "enabled": {{'true' if trend.show_labels else 'false'}},
                                        "align": "center",
                                        "verticalAlign": "top",
                                        "y": -20,
                                        "format": "{{trend.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')}}"
                                },
                                "pointPadding": 0
                            }
                    {% if not loop.last %},{% endif %}
            {% endif %}
        {% endfor %}
        ],

        {% else %}
                {% set unique_metrics = df[['metric', 'fmt']].drop_duplicates() %}
                {% for metric_row in unique_metrics.itertuples() %}
                    {% set filtered_df = df[df['metric'] == metric_row.metric] %}
                    {% set variable_cols = [] %}
                    {% for col in filtered_df.columns %}
                        {% if col not in ['date_column', trend.date_alias, 'month', 'metric', 'fmt', 'chart_name'] %}
                           {% set variable_cols = variable_cols.append(col) %}
                        {% endif %}
                    {% endfor %}
                    {% for col in variable_cols %}
                        {% if col not in ['date_column', trend.date_alias, 'month', 'metric', 'fmt', 'chart_name'] %}
                            {
                                "name": "{{ col }}",
                                "id": "{{ col }} {{ metric_row.metric }}",
                                "data": {{ sfn.replace_nans_with_string_nan(filtered_df[col].to_list()) | tojson }},
                                "fmt": "{{ metric_row.fmt }}",
                                "yAxis": 0,
                                "tooltip": {
                                    "pointFormat": "<span style='color:{point.color}'>\u25CF</span> {{col}}: {{trend.ar_utils.python_to_highcharts_format(metric_row.fmt).get('point_y_format')}}"
                                },
                                "dataLabels": {
                                    "enabled": false,
                                    "align": "center",
                                    "verticalAlign": "top",
                                    "y": -20,
                                    "format": "{{trend.ar_utils.python_to_highcharts_format(metric_row.fmt).get('point_y_format')}}"
                                },
                                "pointPadding": 0
                            }
                            {% if not loop.last %},{% endif %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
                ],
            {% endif %}
    "credits": {
        "enabled": false
    }
}
"""


class TrendAnalysis:
    def __init__(self, table: str, sql_exec: Connector, time: dict, dim_hierarchy: dict = {}, constrained_values={}, max_num_charts=10, df_provider=None):
        self.table = table
        self.sql_exec = sql_exec
        self.date_col = time.get("col")
        self.date_alias = time.get("name") or self.date_col
        self.date_format = time.get("format") or "date_column"
        self.date_sort_col = time.get("sort_col")
        self.pandas_sort_col = "date_order_col" if self.date_sort_col else "date_column"
        self.dim_hier = OldDimensionHierarchy(dim_hierarchy)
        self.helper = SharedFn()
        self.metric_props = {}
        self.allowed_metrics = constrained_values.get("metric", [])
        self.alloed_breakouts = constrained_values.get("breakout", [])
        self.max_num_charts = max_num_charts
        self.row_limit = copy.deepcopy(self.sql_exec.limit)
        self.hit_row_limit = False
        self.hide_totals = False
        self.pull_data_func = df_provider.pull_data if df_provider and hasattr(df_provider, "pull_data") else pull_data

        self.sp = None  # gets set in run_from_env()

        # initialize class variables
        self.facts = pd.DataFrame()
        self.top_facts = pd.DataFrame()
        self.bottom_facts = pd.DataFrame()
        self.underlying_facts = pd.DataFrame()
        self.default_chart = ""
        self.default_table = ""
        self.notes = []
        self.total_col = "Total"
        self.share_within = []
        self.contributions = []
        self.ar_utils = ArUtils()

    @classmethod
    def from_env(cls, env, df_provider=None):

        if env is None:
            raise exit_with_status("env is required")

        cls.env = env

        return cls(
            table=env.trend_parameters["table"],  # name of DB table or parquet table to pull data from
            sql_exec=env.trend_parameters["con"],  # Connector object
            time=env.trend_parameters["time_obj"],  # time object for period aggregation handling, this is how time granularity is determined
            dim_hierarchy=env.trend_parameters["dim_hierarchy"],  # dimension hierarchy required for share trend calculations
            constrained_values=env.trend_parameters["constrained_values"],
            df_provider=df_provider
        )

    # check if skill has all required parameters, if not, return guided message to user
    def check_required_params(self, metrics, metric_props):
        available_metrics = self.allowed_metrics or list(metric_props.keys())
        if not metrics:
            raise exit_with_status(f"Ask user to provide at least one metric. Please do not make choice on user's behalf. Some examples of metrics are: {available_metrics}.")
        elif not all(metric in available_metrics for metric in metrics):
            raise exit_with_status(f"Ask user to specify a valid metric. Please do not make choice on user's behalf. Please ask user to chose from {available_metrics}.")

    # check whether we are hitting row limit from sql execution, its being used to show warning message to user
    def check_row_limit(self, df: pd.DataFrame):
        if not self.hit_row_limit:
            self.hit_row_limit = len(df) == self.sql_exec.limit

    # warning message to user if we are hitting row limit
    def get_row_limit_warning_message(self):
        if self.hit_row_limit:
            return f'<div style="background-color: #FFF8E1; padding: 10px; margin: 10px 0; font-size: 14px; border-radius: 10px;"><strong>⚠ The following analysis has been limited to {self.helper.get_formatted_num(self.sql_exec.limit, ",.0f")} rows which may impact the accuracy of the observations made.</strong></div>'
        else:
            return None

    # helper function to create filter clause
    def single_quoted(self, l: list) -> str:
        if not l: return "()"
        l_out = []
        for s in l:
            # escape single quotes
            s = s.replace("'", "''")
            l_out.append(f"'{s}'")

        return "(" + ",".join(l_out) + ")"

    # helper function for sql generation
    def select_cols(self, cols):
        return ", ".join([col for col in cols if col])

    # helper function to wrap string in quotes
    def wrap_in_quotes(self, name):
        if not name:
            return ''
        else:
            return f'"{name}"'

    # helper function to create a sentece describing the filters applied, metrics used etc for the analysis
    # this is being passed to LLM for fact summarization so that it know what exact analysis is being done
    def get_analysis_message(self, metrics, dims, filters, date_labels, share_title=""):

        # get labels

        met_labels = []
        for metric in metrics:
            met_dict = self.helper.get_metric_prop(metric, self.metric_props)
            met_labels.append(met_dict.get("label", met_dict.get("name")))

        dim_labels = []
        for dim in dims:
            dim_dict = self.helper.get_dimension_prop(dim, self.dim_props)
            dim_labels.append(dim_dict.get("label", dim_dict.get("name")))

        filter_labels = []
        dim_filters, metric_filters = old_split_dim_and_metric_filters(filters, dim_props=self.dim_props)
        for f in dim_filters:
            dim_dict = self.helper.get_dimension_prop(f['col'], self.dim_props)
            filter_labels.append(f"{dim_dict.get('label', dim_dict.get('name'))} {f['op']} {f['val']}")

        for f in metric_filters:
            met_dict = self.helper.get_metric_prop(f['col'], self.metric_props)
            filter_labels.append(f"{met_dict.get('label', met_dict.get('name'))} {f['op']} {f['val']}")

        date_label = old_get_date_label_str(date_labels, prefix="")

        # form message

        analysis_message = f"Analysis ran for metric{'s' if len(met_labels) > 1 else ''} {self.helper.and_comma_join(met_labels)}"
        if dim_labels:
            analysis_message += f" broken out by {self.helper.and_comma_join(dim_labels)}"
        if filter_labels and not share_title:
            analysis_message += f" filtered to {self.helper.and_comma_join(filter_labels)}"
        elif share_title:
            analysis_message += f" with {share_title}"

        if date_label:
            analysis_message += f" for the period {date_label}"

        analysis_message += f" by {self.date_alias}"

        return analysis_message

    # function to get the total for all time period. this is being conditionally displayed in the table
    def calculate_totals_df(self, df, calculated_metrics=[], non_calculated_metrics=[], filters=[], dim=None):

        dim_members = df[dim].unique().tolist() if dim else None

        sum_agg_df = pd.DataFrame()
        calculated_agg_df = pd.DataFrame()

        # seperate additive and non-additive metrics, sum the additive metrics and pull the data for non-additive metrics
        if non_calculated_metrics:
            sum_agg_df = df.groupby(dim) if dim else df
            sum_agg_df = sum_agg_df.agg({metric: "sum" for metric in non_calculated_metrics}).reset_index()

        if calculated_metrics:

            member_filters = [{"col": dim, "op": "IN", "val": dim_members}] if dim and dim_members else []
            metric_in_row_dim = []

            if self.metric_name_column:
                metric_in_row_dim.append(self.metric_name_column)

            dims = [d for d in ([dim] + metric_in_row_dim) if d]

            calculated_agg_df = self.pull_data_func(metrics=calculated_metrics,
                                                    filters=filters + member_filters,
                                                    breakouts=dims)

        # merge dfs
        if dim:
            if not sum_agg_df.empty and not calculated_agg_df.empty:
                totals_df = pd.merge(sum_agg_df, calculated_agg_df, on=dim, how='inner')
            elif not sum_agg_df.empty:
                totals_df = sum_agg_df
            else:
                totals_df = calculated_agg_df
        else:
            if not sum_agg_df.empty:
                sum_agg_df = sum_agg_df.set_index('index').T
            totals_df = pd.concat([sum_agg_df, calculated_agg_df], axis=1)

        # add columns to match df structure
        totals_df["date_column"] = np.nan
        totals_df[self.date_alias] = self.total_col
        if self.date_sort_col:
            totals_df[self.pandas_sort_col] = np.nan

        return totals_df

    # function to get the metric sql for the analysis
    def get_metric_sql(self, metric):
        calculated_metrics, non_calculated_metrics = None, None
        if metric.get("sql") and metric.get("col"):
            calculated_metrics = metric
        elif metric.get("col"):
            non_calculated_metrics = metric["name"]
        return calculated_metrics, non_calculated_metrics

    # function to get the growth data for the analysis, being overriden in the child class (AdHocTrendAnalysis)
    def get_growth_data(self, df, metrics):
        return df, metrics

    def get_referenced_table_and_starting_sql(self, table, view):

        if view:
            unique_table = table
            sql = f"WITH {table} AS ({view}),"
        else:
            # avoiding a recursive CTE by creating a unique table name
            unique_table = f"{table}_view"
            sql = f"WITH {unique_table} AS (SELECT * FROM {table}),"

        return unique_table, sql

    def is_agg_metric(self, metric):
        # List of common SQL aggregate functions
        agg_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDEVP', 'STDEV', "SHARE"]

        # Regular expression pattern to match aggregate functions with any characters between the function and parentheses
        # (case-insensitive)
        pattern = re.compile(fr'\b(?:{"|".join(agg_functions)})\b\s*\([^)]*\)', re.IGNORECASE)

        # Check if the metric contains any aggregate function
        return bool(pattern.search(metric))

    # function to get the trend data for the analysis
    def get_trend_data(self, metrics, dims, filters, top_n, top_n_direction, required_dim_vals):
        use_max_sql_gen = is_using_max_sql_gen()

        referenced_table, sql = self.get_referenced_table_and_starting_sql(self.table, self.view)

        # process dim filters
        for f in filters:
            for dim in dims:
                if f['col'] == dim and f['op'].lower() in ['=', 'in']:
                    # make sure all dim filters are also required dim vals
                    if isinstance(f['val'], list):
                        required_dim_vals[dim].extend(f['val'])
                    else:
                        required_dim_vals[dim].append(f['val'])
                    # remove dim ambiguity
                    f['col'] = f"{referenced_table}.{dim}" if not use_max_sql_gen else dim
                    break

        # format metric sql
        calculated_metrics = []
        non_calculated_metrics = []

        if self.metric_name_column:
            c_metrics, nc_metrics = self.get_metric_sql(self.value_metric)
            if nc_metrics:
                non_calculated_metrics.append(nc_metrics)
            else:
                calculated_metrics.append(c_metrics)
            dims.append(self.metric_name_column)
        else:
            for metric in metrics:
                c_metrics, nc_metrics = self.get_metric_sql(metric)
                if nc_metrics:
                    non_calculated_metrics.append(nc_metrics)
                else:
                    calculated_metrics.append(c_metrics)

        if dims:
            # Get the data for the top 10 dim values (+ required dim values) by the first metric for each dimension
            dim_dfs = []

            for dim in dims:
                top_dim_vals = []
                if top_n:
                    first_metric = metrics[0]
                    top_dim_df = self.pull_data_func(metrics=[first_metric],
                                                     filters=filters,
                                                     breakouts=[dim],
                                                     order_cols=[{"col": first_metric["name"], "direction": top_n_direction}],
                                                     query_row_limit=top_n)

                    top_dim_vals = list(top_dim_df[dim].unique())

                if required_dim_vals.get(dim):
                    top_dim_vals = [v.lower() for v in top_dim_vals + required_dim_vals[dim]]
                    top_dim_vals = list(dict.fromkeys(top_dim_vals))
                else:
                    top_dim_vals = top_dim_vals

                if top_dim_vals:
                    datapull_filters = filters + [{"col": dim, "op": "IN", "val": top_dim_vals}]
                else:
                    datapull_filters = filters

                df = self.pull_data_func(metrics=metrics,
                                         filters=datapull_filters,
                                         breakouts=[dim] + [f"max_time_{self.time_granularity}"],
                                         order_cols=[{"col": f"max_time_{self.time_granularity}", "alias": "date_column", "direction": "ASC"}],
                                         query_row_limit=self.row_limit)

                df.rename(columns={f"max_time_{self.time_granularity}": self.time_granularity}, inplace=True)

                self.check_row_limit(df)

                # calculate totals
                if not self.hide_totals:
                    totals_df = self.calculate_totals_df(df, calculated_metrics=calculated_metrics, non_calculated_metrics=non_calculated_metrics, filters=filters, dim=dim)
                    df = pd.concat([df, totals_df], axis=0)

                # rename dim values to 'dim_val', create dim column
                df['dim'] = dim
                df.rename(columns={dim: 'dim_val'}, inplace=True)

                dim_dfs.append(df)

            df = pd.concat(dim_dfs, axis=0)

        else:
            df = self.pull_data_func(metrics=metrics,
                                     filters=filters,
                                     breakouts=[f"max_time_{self.time_granularity}"],
                                     order_cols=[{"col": f"max_time_{self.time_granularity}", "alias": "date_column", "direction": "ASC"}],
                                     query_row_limit=self.row_limit)
            df.rename(columns={f"max_time_{self.time_granularity}": self.time_granularity}, inplace=True)

            self.check_row_limit(df)

            # calculate totals
            if not self.hide_totals:
                totals_df = self.calculate_totals_df(df, calculated_metrics=calculated_metrics, non_calculated_metrics=non_calculated_metrics, filters=filters)
                df = pd.concat([df, totals_df], axis=0)

        if dims:
            id_vars = ['date_column', self.date_alias, 'dim_val', 'dim']
            df['dim'] = df['dim'].astype(str)
        else:
            id_vars = ['date_column', self.date_alias]

        if self.date_sort_col:
            id_vars.append(self.pandas_sort_col)

        metric_cols = [metric["name"] for metric in metrics]
        if not self.metric_name_column:
            df = df.melt(id_vars=id_vars, value_vars=metric_cols, var_name='metric', value_name='value')
        else:
            df = df.rename(columns={self.metric_name_column: 'metric', self.value_metric["col"]: 'value'})

        df['value'] = df['value'].astype(float)

        # sql query forces lowercase cols, revert back to original case
        metric_map = {metric.lower(): metric for metric in metric_cols}
        df['metric'] = df['metric'].apply(lambda x: metric_map.get(x, x))

        print(df.head().to_string())

        if not self.date_sort_col:
            df["date_column"] = pd.to_datetime(df["date_column"])

        if len(df) > 0:
            self.actual_first_period, self.actual_last_period = df[self.date_alias].iloc[0], df[self.date_alias].iloc[-1]
        else:
            self.actual_first_period, self.actual_last_period = "", ""

        self.show_labels = len(df["date_column"].unique()) < 25

        return df

    # defaul jinja template and styling being used for table
    def get_default_table_jinja(self):
        return """
            <head>
                <meta charset="UTF-8">
                <style>
                body {
                    font-size:12pt;
                    font-family: Arial, sans-serif;
                    line-height:1.5;
                }
                table {
                    border: 0;
                    width: 100%;
                    table-layout: fixed;
                    font-size: 10pt;
                    border-collapse: collapse;
                }
                thead {
                    background: #EEE;
                    height: 25px;
                }
                tbody tr:nth-child(odd) {
                    background-color: #f9f9f9;
                }
                tbody tr:nth-child(even) {
                    background-color: #ffffff;
                }
                th {
                    text-align: right;
                    height: 30px;
                    padding-right: 5px;
                }
                tr {
                    display: table-row;
                }
                td {
                    text-align: right;
                    padding-right: 5px;
                }
                .daily_values {

                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                }
                th {
                    border-bottom: 1px solid #c0c0c0;
                    padding: 8px;
                    text-align: right;
                }
                td {
                    border-bottom: 1px solid #e0e0e0;
                    padding: 8px;
                    text-align: right;
                }
                th {
                    background-color: #f2f2f2;
                }
                </style>
            </head>
            <body>
                <div style="width: 100%; overflow-x:auto;">
                    <table style="min-width:100%; width:auto; white-space:nowrap;">
                        <thead>
                        <tr>
                            {% for col in df.columns %}
                            <th>{{ col }}</th>
                            {% endfor %}
                        </tr>
                        </thead>
                        <tbody>
                        {% for index, row in df.iterrows() %}
                        <tr>
                            {% for item in row %}
                            <td>{{ item }}</td>
                            {% endfor %}
                        </tr>
                        {% endfor %}
                        </tbody>
                    </table>
                </div>
            </body>
        """

    # render the jija template using actual dataframe
    def get_default_table(self, df):
        table_template = self.get_default_table_jinja()
        j2_vars = {"df": df}
        rendered_table = self.helper.j2_render(table_template, j2_vars)
        return rendered_table
    
    def get_series_type(self, metric):
        if metric.endswith("YoY Difference") or metric.endswith("PoP Difference"):
            return 'difference'
        elif metric.endswith("YoY % Change") or metric.endswith("YoY Growth") or metric.endswith("PoP % Change") or metric.endswith("PoP Growth"):
            return 'growth'
        else:
            return 'absolute'
        
    def get_variance_series_type(self, metric, variance_type):
        if metric.endswith(f"% from {variance_type.title()}") or metric.endswith(f"Growth from {variance_type.title()}"):
            return 'growth'
        elif metric.endswith(f"from {variance_type.title()}"):
            return 'difference'
        else:
            return 'absolute'

    # render the chart template for actual dataframe, this will output the chart JSON
    def get_default_chart_jinja(self, render_jinja=False):

        template = CHART_TEMPLATE

        if not render_jinja:
            return template

        rendered_charts = defaultdict(list)
        growth_type = str(self.env.growth_type).lower()
        for name in self.display_charts.keys():
            display_df = self.display_charts[name]['df']
            if growth_type in ["y/y", "p/p"]:
                if self.env.breakouts:
                    unique_metrics = list(display_df['metric'].unique())
                    for metric in unique_metrics:
                        original_metric = metric.replace("YoY Difference", "").replace("YoY % Change", "").strip()
                        df = display_df[display_df['metric'] == metric]
                        series = self.get_series_type(metric)
                        j2_vars = {
                            "df": df,
                            "sfn": self.helper,
                            "trend": self,
                            "metric_props": self.metric_props,
                            "axis_type": series,
                            "name": metric
                        }
                        rendered_charts[name].append(self.helper.j2_render(CHART_TEMPLATE, j2_vars))
                else:
                    growth_metrics = [m for m in display_df.columns if m.endswith("_growth")]
                    difference_metrics = [m for m in display_df.columns if m.endswith("_delta")]
                    absolute_metrics = [m.replace("_delta", "") for m in difference_metrics]
                    series_map = {"absolute": absolute_metrics, "difference": difference_metrics, "growth": growth_metrics}
                    for series, metrics in series_map.items():
                        keep_cols = ['date_column', 'month', 'quarter', 'year', 'chart_name'] + metrics
                        keep_cols = [col for col in keep_cols if col in display_df.columns]
                        df = display_df[keep_cols]
                        j2_vars = {
                            "df": df,
                            "sfn": self.helper,
                            "trend": self,
                            "metric_props": self.metric_props,
                            "axis_type": series,
                            "name": series.title()
                        }
                        rendered_charts[name].append(self.helper.j2_render(CHART_TEMPLATE, j2_vars))
            else:
                display_df['chart_name'] = name
                df = display_df
                j2_vars = {
                    "df": df,
                    "sfn": self.helper,
                    "trend": self,
                    "metric_props": self.metric_props,
                    "name": ""
                }
                rendered_charts[name].append(self.helper.j2_render(NON_GROWTH_CHART_TEMPLATE, j2_vars))
        return rendered_charts
    
    def get_dynamic_layout_chart_vars(self):
        rendered_charts = defaultdict(dict)
        growth_type = str(self.env.growth_type).lower()
        # this is looping through tabs
        for name in self.display_charts.keys():
            display_df = self.display_charts[name]['df']
            footer = self.display_charts[name].get("footnote")
            if growth_type in ["y/y", "p/p"]:
                if self.env.breakouts:
                    unique_metrics = list(display_df['metric'].unique())
                    chart_vars = {"hide_growth_chart": False, "hide_absolute_series_name": False, "footer": footer, "hide_footer": False if footer else True}
                    for metric in unique_metrics:
                        df = display_df[display_df['metric'] == metric]
                        
                        series_type = self.get_series_type(metric)
                        chart_vars[f"{series_type}_metric_name"] = metric

                        # collect vars
                        chart_name = df["chart_name"].iloc[0]
                        chart_obj = self.display_charts[chart_name]
                        x_axis = chart_obj['xAxis']
                        y_axis = []
                        for axis in chart_obj['axis_info'][series_type]["yaxis"]:
                            y_axis.append({
                                "title": axis['title'],
                                "labels": {
                                    "format": self.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')},
                                "opposite": True if axis['opposite'] else False
                            })

                        series = []

                        unique_metrics = df[['metric', 'fmt']].drop_duplicates()
                        for metric_row in unique_metrics.itertuples():
                            filtered_df = df[df['metric'] == metric_row.metric]
                            variable_cols = []
                            for col in filtered_df.columns:
                                if col not in ['date_column', self.date_alias, 'month', 'metric', 'fmt', 'chart_name']:
                                    variable_cols.append(col)

                            for col in variable_cols:
                                if col not in ['date_column', self.date_alias, 'chart_name']:
                                    series_name = col
                                    point_formatter = self.ar_utils.python_to_highcharts_format(metric_row.fmt).get('point_y_format')
                                    dataLabels = {
                                        "enabled": True if self.show_labels else False,
                                        "align": "center",
                                        "verticalAlign": "top",
                                        "y": -20,
                                        "format": point_formatter
                                    },

                                    series.append({"name": series_name,
                                                   "id": f"{series_name}_{metric_row.metric}",
                                                   "fmt": metric_row.fmt,
                                                   "data": self.helper.replace_nans_with_string_nan(df[col].to_list()),
                                                   "yAxis": 0,
                                                   "tooltip": {"pointFormat": "<b>{series.name}</b>: " + point_formatter},
                                                   "dataLabels": dataLabels,
                                                   "pointPadding": 0})

                        chart_vars[f"{series_type}_series"] = series
                        chart_vars[f"{series_type}_x_axis_categories"] = x_axis
                        chart_vars[f"{series_type}_y_axis"] = y_axis
                        chart_vars[f"{series_type}_meta_df_id"] = display_df.max_metadata.get_id()
                    rendered_charts[name] = chart_vars
                else:
                    growth_metrics = [m for m in display_df.columns if m.endswith("_growth")]
                    difference_metrics = [m for m in display_df.columns if m.endswith("_delta")]
                    absolute_metrics = [m.replace("_delta", "") for m in difference_metrics]
                    series_map = {"absolute": absolute_metrics, "difference": difference_metrics, "growth": growth_metrics}
                    chart_vars = {"hide_growth_chart": False, "hide_absolute_series_name": False, "footer": footer, "hide_footer": False if footer else True}
                    for series_type, metrics in series_map.items():
                        keep_cols = ['date_column', 'month', 'quarter', 'year', 'chart_name'] + metrics
                        keep_cols = [col for col in keep_cols if col in display_df.columns]
                        df = display_df[keep_cols]

                        chart_name = df["chart_name"].iloc[0]
                        chart_obj = self.display_charts[chart_name]
                        x_axis = chart_obj['xAxis']
                        y_axis = []
                        for axis in chart_obj['axis_info'][series_type]["yaxis"]:
                            y_axis.append({
                                "title": axis['title'],
                                "labels": {
                                    "format": self.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')},
                                "opposite": True if axis['opposite'] else False
                            })
                        series = []
                        for col in df.columns:
                            if col not in ['date_column', self.date_alias, 'month', 'metric', 'fmt', 'chart_name']:
                                series_name = chart_obj['name'] if chart_obj['name'] else self.helper.get_metric_prop(col, self.metric_props)['label']
                                num_format = chart_obj['fmt'] if chart_obj['fmt'] else self.helper.get_metric_prop(col, self.metric_props)['fmt']

                                data = self.helper.replace_nans_with_string_nan(df[col].to_list())
                                point_formatter = self.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')

                                dataLabels = {
                                    "enabled": True if self.show_labels else False,
                                    "align": "center",
                                    "verticalAlign": "top",
                                    "y": -20,
                                    "format": point_formatter
                                },

                                series.append({"name": series_name,
                                               "id": f"{series_name}",
                                               "data": data,
                                               "yAxis": chart_obj["yaxis_index"][col],
                                               "tooltip": {"pointFormat": "<b>{series.name}</b>: " + point_formatter},
                                               "dataLabels": dataLabels,
                                               "pointPadding": 0})

                        chart_vars[f"{series_type}_series"] = series
                        chart_vars[f"{series_type}_x_axis_categories"] = x_axis
                        chart_vars[f"{series_type}_y_axis"] = y_axis
                        chart_vars[f"{series_type}_metric_name"] = series_name if len(metrics) == 1 else series_type.title()
                        chart_vars[f"{series_type}_meta_df_id"] = display_df.max_metadata.get_id()

                    rendered_charts[name] = chart_vars
            
            elif growth_type in VARIANCE_TYPES:
                variance_type = self.get_variance_type(self.env.growth_type)
                if self.env.breakouts:
                    unique_metrics = list(display_df['metric'].unique())
                    chart_vars = {"hide_growth_chart": False, "hide_absolute_series_name": False, "footer": footer, "hide_footer": False if footer else True}
                    for metric in unique_metrics:
                        df = display_df[display_df['metric'] == metric]
                        
                        series_type = self.get_variance_series_type(metric, variance_type)
                        chart_vars[f"{series_type}_metric_name"] = metric

                        # collect vars
                        chart_name = df["chart_name"].iloc[0]
                        chart_obj = self.display_charts[chart_name]
                        x_axis = chart_obj['xAxis']
                        y_axis = []
                        for axis in chart_obj['axis_info'][series_type]["yaxis"]:
                            y_axis.append({
                                "title": axis['title'],
                                "labels": {
                                    "format": self.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')},
                                "opposite": True if axis['opposite'] else False
                            })

                        series = []

                        unique_metrics = df[['metric', 'fmt']].drop_duplicates()
                        for metric_row in unique_metrics.itertuples():
                            filtered_df = df[df['metric'] == metric_row.metric]
                            variable_cols = []
                            for col in filtered_df.columns:
                                if col not in ['date_column', self.date_alias, 'month', 'metric', 'fmt', 'chart_name']:
                                    variable_cols.append(col)

                            for col in variable_cols:
                                if col not in ['date_column', self.date_alias, 'chart_name']:
                                    series_name = col
                                    point_formatter = self.ar_utils.python_to_highcharts_format(metric_row.fmt).get('point_y_format')
                                    dataLabels = {
                                        "enabled": True if self.show_labels else False,
                                        "align": "center",
                                        "verticalAlign": "top",
                                        "y": -20,
                                        "format": point_formatter
                                    },

                                    series.append({"name": series_name,
                                                   "id": f"{series_name}_{metric_row.metric}",
                                                   "fmt": metric_row.fmt,
                                                   "data": self.helper.replace_nans_with_string_nan(df[col].to_list()),
                                                   "yAxis": 0,
                                                   "tooltip": {"pointFormat": "<b>{series.name}</b>: " + point_formatter},
                                                   "dataLabels": dataLabels,
                                                   "pointPadding": 0})

                        chart_vars[f"{series_type}_series"] = series
                        chart_vars[f"{series_type}_x_axis_categories"] = x_axis
                        chart_vars[f"{series_type}_y_axis"] = y_axis
                        chart_vars[f"{series_type}_meta_df_id"] = display_df.max_metadata.get_id()
                    rendered_charts[name] = chart_vars
                else:

                    variance_metrics = [m for m in display_df.columns if m.endswith(variance_type)]
                    delta_metrics = [m for m in display_df.columns if m.endswith(VARIANCE_DELTA)]
                    percent_delta_metrics = [m for m in display_df.columns if m.endswith(VARIANCE_GROWTH)]
                    absolute_metrics = [m.replace(f"_{variance_type}", "") for m in variance_metrics]

                    def map_series_type_to_name(series_type, variance_type):
                        if series_type == "absolute":
                            return "Absolute"
                        elif series_type == "difference":
                            return f"Trend of Variance from {variance_type.title()}"
                        elif series_type == "growth":
                            return f"Trend of Variance Growth from {variance_type.title()}"

                    series_map = {"absolute": absolute_metrics, "difference": delta_metrics, "growth": percent_delta_metrics}
                    chart_vars = {"hide_growth_chart": False, "hide_absolute_series_name": False, "footer": footer, "hide_footer": False if footer else True}
                    for series_type, metrics in series_map.items():
                        keep_cols = ['date_column', 'month', 'quarter', 'year', 'chart_name'] + metrics
                        keep_cols = [col for col in keep_cols if col in display_df.columns]
                        df = display_df[keep_cols]

                        chart_name = df["chart_name"].iloc[0]
                        chart_obj = self.display_charts[chart_name]
                        x_axis = chart_obj['xAxis']
                        y_axis = []
                        for axis in chart_obj['axis_info'][series_type]["yaxis"]:
                            y_axis.append({
                                "title": axis['title'],
                                "labels": {
                                    "format": self.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')},
                                "opposite": True if axis['opposite'] else False
                            })
                        series = []
                        for col in df.columns:
                            if col not in ['date_column', self.date_alias, 'month', 'metric', 'fmt', 'chart_name']:
                                series_name = chart_obj['name'] if chart_obj['name'] else self.helper.get_metric_prop(col, self.metric_props)['label']
                                num_format = chart_obj['fmt'] if chart_obj['fmt'] else self.helper.get_metric_prop(col, self.metric_props)['fmt']

                                data = self.helper.replace_nans_with_string_nan(df[col].to_list())
                                point_formatter = self.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')

                                dataLabels = {
                                    "enabled": True if self.show_labels else False,
                                    "align": "center",
                                    "verticalAlign": "top",
                                    "y": -20,
                                    "format": point_formatter
                                },

                                series.append({"name": series_name,
                                               "id": f"{series_name}",
                                               "data": data,
                                               "yAxis": chart_obj["yaxis_index"][col],
                                               "tooltip": {"pointFormat": "<b>{series.name}</b>: " + point_formatter},
                                               "dataLabels": dataLabels,
                                               "pointPadding": 0})

                        chart_vars[f"{series_type}_series"] = series
                        chart_vars[f"{series_type}_x_axis_categories"] = x_axis
                        chart_vars[f"{series_type}_y_axis"] = y_axis
                        chart_vars[f"{series_type}_metric_name"] = series_name if len(metrics) == 1 else map_series_type_to_name(series_type, variance_type)
                        chart_vars[f"{series_type}_meta_df_id"] = display_df.max_metadata.get_id()

                    rendered_charts[name] = chart_vars
            else:
                chart_vars = {"hide_growth_chart": True, "hide_absolute_series_name": True, "footer": footer, "hide_footer": False if footer else True}
                display_df['chart_name'] = name
                df = display_df

                chart_name = df["chart_name"].iloc[0]
                chart_obj = self.display_charts[chart_name]
                x_axis = df[self.date_alias].to_list()
                y_axis = []
                for axis in chart_obj["yaxis"]:
                    y_axis.append({"title": axis['title'], "labels": {
                        "format": self.ar_utils.python_to_highcharts_format(axis['fmt']).get('value_format')},
                                   "opposite": True if axis['opposite'] else False})

                variable_cols = []
                for col in df.columns:
                    if col not in ['date_column', self.date_alias, 'month', 'metric', 'fmt', 'chart_name']:
                        variable_cols.append(col)

                series = []
                series_type = "absolute"
                for col in variable_cols:
                    if col not in ['date_column', self.date_alias, 'chart_name', 'fmt']:
                        if not chart_obj["dim_breakout"]:
                            line_name = chart_obj['name'] if chart_obj['name'] else \
                                self.helper.get_metric_prop(col, self.metric_props)['label']
                            num_format = chart_obj['fmt'] if chart_obj['fmt'] else \
                                self.helper.get_metric_prop(col, self.metric_props)['fmt']
                        else:
                            num_format = chart_obj['fmt'] if chart_obj['fmt'] else \
                                self.helper.get_metric_prop(col, self.metric_props)['fmt']
                            line_name = chart_obj['name'] if chart_obj['name'] else col

                        point_format = self.ar_utils.python_to_highcharts_format(num_format).get('point_y_format')

                        series.append({"name": line_name,
                                       "data": self.helper.replace_nans_with_string_nan(df[col].to_list()),
                                       "yAxis": chart_obj["yaxis_index"][col] if not chart_obj["dim_breakout"] else 0,
                                       "tooltip": {"pointFormat": "<b>{series.name}</b>: " + point_format},
                                       "dataLabels": {"enabled": True if self.show_labels else False,
                                                      "align": "center",
                                                      "verticalAlign": "top",
                                                      "y": -20,
                                                      "format": point_format},
                                       "pointPadding": 0
                                       })

                chart_vars[f"{series_type}_series"] = series
                chart_vars[f"{series_type}_x_axis_categories"] = x_axis
                chart_vars[f"{series_type}_y_axis"] = y_axis
                chart_vars[f"{series_type}_metric_name"] = ""
                chart_vars[f"{series_type}_meta_df_id"] = display_df.max_metadata.get_id()

                rendered_charts[name] = chart_vars

        return rendered_charts

    # function to get the chart data for the analysis, being overriden in the child class (AdHocTrendAnalysis)
    def get_charts(self, df, metrics, dims):
        return self._get_charts(df, metrics, dims)

    # for each metric and dim combination, create a chart
    # this function is consolidating all the metadata needed for chart rendering
    # the logic is if there is only one metric and no dim, then create a single chart
    # if there is only one metric and multiple dims, then create a chart for each dim
    # if there are multiple metrics and no dim, then create a single chart with multiple axis
    # percentage metrics are always on the right axis, only one axis for all percentage metrics
    def _get_charts(self, orig_df, metrics, dims):
        df = copy.deepcopy(orig_df)
        ## each chart is a dictionary that gets referenced in the jinja template by its key (name of chart)
        charts = {}

        axis_grouping_rules = self.env.trend_parameters.get('axis_grouping')


        if len(dims) == 0 and len(metrics) == 1:

            dim_breakout = False
            fmt = None
            yaxis = []
            yaxis_index = {}

            met_prop = self.helper.get_metric_prop(metrics[0], self.metric_props)

            fmt = met_prop['fmt']
            yaxis.append(
                {
                    "title": met_prop.get('label', metrics[0]),
                    "fmt": fmt,
                    "pretty_num": False if '%' in fmt else True,
                    "opposite": False
                }
            )
            yaxis_index["value"] = 0

            drop_cols = ['metric']
            if "date_order_col" in df.columns:
                drop_cols.append("date_order_col")
            df.drop(columns=drop_cols, inplace=True)

            chart_name = "Trend Chart"
            df['chart_name'] = chart_name

            charts[chart_name] = {
                "df": df,
                "yaxis": yaxis,
                "yaxis_index": yaxis_index,
                "dim_breakout": dim_breakout,
                "fmt": fmt,
                "name": self.helper.get_metric_prop(metrics[0], self.metric_props)['label']
            }

        # TODO: Mapping metric formats to axis groups
        elif len(dims) == 0 and len(metrics) > 1:

            dim_breakout = False
            fmt = None
            yaxis = []
            yaxis_index = {}

            index_cols = ['date_column', self.date_alias]
            if 'date_order_col' in df.columns:
                index_cols.insert(0, 'date_order_col')  # index by date_order_col first since df is sorted by this col

            df = df.pivot(index=index_cols, columns='metric', values='value').reset_index()

            if "date_order_col" in df.columns:
                df = df.drop(columns=["date_order_col"])

            # group metrics into axis
            pct_metrics = [metric for metric in metrics if '%' in self.helper.get_metric_prop(metric, self.metric_props)['fmt']]
            non_pct_metrics = [metric for metric in metrics if metric not in pct_metrics]

            # if no pct metrics, split metrics into left and right axis
            if len(pct_metrics) == 0 and len(non_pct_metrics) > 0:
                split_index = (len(non_pct_metrics) + 1) // 2
                right_axis_vals = non_pct_metrics[split_index:]

                for idx, metric in enumerate(metrics):
                    yaxis.append(
                        {
                            "title": self.helper.get_metric_prop(metric, self.metric_props).get('label', metric),
                            "fmt": self.helper.get_metric_prop(metric, self.metric_props)['fmt'],
                            "pretty_num": True,
                            "opposite": True if metric in right_axis_vals else False
                        }
                    )
                    yaxis_index[metric] = idx

            # all metrics are %, use left axis, all share same axis
            elif len(non_pct_metrics) == 0 and len(pct_metrics) > 0:
                if len(pct_metrics) > 1:
                    title = ""
                else:
                    title = self.helper.get_metric_prop(pct_metrics[0], self.metric_props).get('label', pct_metrics[0])
                fmt = self.helper.get_metric_prop(pct_metrics[0], self.metric_props)['fmt']
                yaxis_index = {metric: 0 for metric in pct_metrics}
                yaxis.append(
                    {
                        "title": title,
                        "fmt": fmt,
                        "pretty_num": False,
                        "opposite": False
                    }
                )

            # mix of % and non-% metrics, use left axis for non-% and right axis for %. All % share same axis
            else:
                title = "" if len(pct_metrics) > 1 else self.helper.get_metric_prop(pct_metrics[0], self.metric_props).get('label', pct_metrics[0])
                fmt = self.helper.get_metric_prop(pct_metrics[0], self.metric_props)['fmt']
                yaxis_index = {metric: 0 for metric in pct_metrics}
                yaxis.append(
                    {
                        "title": title,
                        "fmt": fmt,
                        "pretty_num": False,
                        "opposite": True
                    }
                )

                for idx, metric in enumerate(non_pct_metrics):
                    idx += 1
                    if metric in non_pct_metrics:
                        yaxis.append(
                            {
                                "title": self.helper.get_metric_prop(metric, self.metric_props)['label'],
                                "fmt": self.helper.get_metric_prop(metric, self.metric_props)['fmt'],
                                "pretty_num": True,
                                "opposite": False
                            }
                        )
                        yaxis_index[metric] = idx

            chart_name = "Trend Chart"
            df['chart_name'] = chart_name

            charts[chart_name] = {
                "df": df,
                "yaxis": yaxis,
                "yaxis_index": yaxis_index,
                "dim_breakout": dim_breakout,
                "fmt": None,
                "name": None
            }

        else:

            for metric in metrics:
                for dim in dims:

                    # a new chart is created each loop
                    dim_breakout = True
                    fmt = None
                    yaxis = []
                    yaxis_index = {}

                    chart_df = df.copy()

                    # remove other metrics and other dims
                    chart_df = chart_df[(chart_df['metric'] == metric) & (chart_df['dim'] == dim)]
                    chart_df_metric_value = chart_df['value']
                    chart_df.drop(columns=['metric', 'value', 'dim'], inplace=True)
                    chart_df[metric] = chart_df_metric_value

                    # pivot chart_df so the values of the dim are columns, the jinja template expects it this way
                    index_cols = ['date_column', self.date_alias]
                    if 'date_order_col' in df.columns:
                        index_cols.insert(0, 'date_order_col')  # index by date_order_col first since df is sorted by this col

                    chart_df = chart_df.pivot(index=index_cols, columns='dim_val', values=metric).reset_index()

                    if "date_order_col" in chart_df.columns:
                        chart_df = chart_df.drop(columns=["date_order_col"])

                    met_prop = self.helper.get_metric_prop(metric, self.metric_props)
                    dim_prop = self.helper.get_dimension_prop(dim, self.dim_props)

                    fmt = met_prop['fmt']
                    yaxis.append(
                        {
                            "title": met_prop.get('label', metric),
                            "fmt": fmt,
                            "pretty_num": False if '%' in fmt else True,
                            "opposite": False
                        }
                    )

                    # get chart name and store chart
                    met_label = met_prop.get("label", met_prop.get("name"))
                    dim_label = dim_prop.get("label", dim_prop.get("name"))

                    chart_name = f'{dim_label} · {met_label}' if len(dims) > 1 and len(metrics) > 1 else (dim_label if len(dims) > 1 else met_label)
                    chart_df['chart_name'] = chart_name

                    # add footer about share calculation
                    footnote = None
                    if met_prop.get("metric_type") == "share":
                        if self.footnotes.get(dim_label):
                            footnote = self.footnotes[dim_label]

                    charts[chart_name] = {
                        "df": chart_df,
                        "yaxis": yaxis,
                        "yaxis_index": yaxis_index,
                        "dim_breakout": dim_breakout,
                        "fmt": fmt,
                        "name": None,
                        "footnote": footnote
                    }
        return charts

    # function to get the facts for the analysis, being overriden in the child class (AdHocTrendAnalysis)
    def get_facts_df(self, df, top_growth=False, asc=False, top_n=3):
        return self._get_facts_df(df, top_growth, asc, top_n)

    # function to get the facts for the analysis, this function is adding the summary columns to the facts dataframe
    def _get_facts_df(self, orig_df, top_growth=False, asc=False, top_n=3, do_not_format=False, is_growth=False):
        df = orig_df.copy()

        # summary columns
        summary_cols = ['metric']

        if 'dim' in df.columns:
            summary_cols.extend(['dim', 'dim_val'])

        if "date_order_col" in df.columns:
            df.drop(columns=["date_order_col"], inplace=True)

        # group by metric col
        group_dfs = df.groupby(['metric', 'dim']) if 'dim' in df.columns else df.groupby('metric')
        dfs = []

        # preserve ordering of date alias columns
        date_alias_column_ordering = df[self.date_alias].unique()
        first_period_col = [x for x in date_alias_column_ordering if x != self.total_col][0]
        last_period_col = [x for x in date_alias_column_ordering if x != self.total_col][-1]

        for groups, df in group_dfs:

            # preserve ordering of date alias columns
            if is_growth:
                # skip adding facts for metric that has all 0s
                if len(df[~pd.isnull(df["value"])]) == 0:
                    continue
                date_alias_column_ordering = df[~pd.isnull(df["value"])][self.date_alias].unique()
                first_period_col = [x for x in date_alias_column_ordering if x != self.total_col][0]
                last_period_col = [x for x in date_alias_column_ordering if x != self.total_col][-1]

            metric, dim = groups if 'dim' in df.columns else (groups, None)

            met_prop = self.helper.get_metric_prop(metric, self.metric_props)

            if metric.endswith(GROWTH) and met_prop.get("hide_percentage_change"):
                continue

            fmt = met_prop['fmt']

            index_cols = ['metric', 'dim', 'dim_val'] if 'dim' in df.columns else ['metric']

            df = df.pivot(index=index_cols, columns=self.date_alias, values='value')
            df = df.reindex(date_alias_column_ordering, axis=1)  # use preserved ordering for the date alias columns
            df = df.reset_index()

            df.dropna(axis=1, how='all', inplace=True)

            df['metric'] = met_prop.get('label', metric)
            if 'dim' in df.columns:
                df['dim'] = self.helper.get_dimension_prop(dim, self.dim_props).get('label', dim) if dim else None

            df = df.set_index(index_cols)
            df = df.astype(float)

            # total difference calculation
            diff_col = None
            if len(df.columns) > 1:
                diff_col = f"{first_period_col} to {last_period_col} Difference"

                if top_growth:
                    try:
                        # keep only the first and last columns
                        df = df[[first_period_col, last_period_col]]
                        df[diff_col] = df[last_period_col] - df[first_period_col]
                    except:
                        df[diff_col] = np.nan

                    # set diff col to float
                    df[diff_col] = df[diff_col].astype(float)

                    # order by the difference column and get top or bottom n by growth
                    if len(df) > 1:
                        if asc:
                            df = df.nsmallest(top_n, diff_col)
                        else:
                            df = df.nlargest(top_n, diff_col)
                else:
                    date_cols = [d for d in date_alias_column_ordering if self.total_col.lower() != str(d).lower()]
                    # create facts around highest and lowest values in the trend
                    available_date_cols = [col for col in date_cols if col in df.columns]
                    df["highest_val_date"] = df[available_date_cols].idxmax(axis=1, numeric_only=True).astype(str)
                    df["lowest_val_date"] = df[available_date_cols].idxmin(axis=1, numeric_only=True).astype(str)
                    df["Highest Value"] = df[available_date_cols].max(axis=1, numeric_only=True)
                    df["Lowest Value"] = df[available_date_cols].min(axis=1, numeric_only=True)

                    # if dim breakout used, use both dim name and metric in strings
                    if dim:
                        df["Highest Value"] = df.apply(lambda x: f"Highest {x.name[1]} value for {x.name[0]} was on {x['highest_val_date']} with value {self.helper.get_formatted_num(x['Highest Value'], fmt, pretty_num=True)}", axis=1)
                        df["Lowest Value"] = df.apply(lambda x: f"Lowest {x.name[1]} value for {x.name[0]} was on {x['lowest_val_date']} with value {self.helper.get_formatted_num(x['Lowest Value'], fmt, pretty_num=True)}", axis=1)
                    else:
                        df["Highest Value"] = df.apply(lambda x: f"Highest {x.name} value was on {x['highest_val_date']} with value {self.helper.get_formatted_num(x['Highest Value'], fmt, pretty_num=True)}", axis=1)
                        df["Lowest Value"] = df.apply(lambda x: f"Lowest {x.name} value was on {x['lowest_val_date']} with value {self.helper.get_formatted_num(x['Lowest Value'], fmt, pretty_num=True)}", axis=1)

                    df = df.drop(columns=["highest_val_date", "lowest_val_date"])

                    # calculate growth
                    try:
                        df[diff_col] = df[last_period_col] - df[first_period_col]
                    except:
                        df[diff_col] = np.nan

                    if not metric.endswith(GROWTH) and not metric.endswith(DELTA):
                        summary_cols.extend([first_period_col, last_period_col, diff_col, "Highest Value", "Lowest Value"])

                    if self.total_col in df.columns:
                        summary_cols.append(self.total_col)

            elif len(df.columns) == 1:
                summary_cols.append(df.columns[0])

            # rank the last period values and rank of the difference
            if last_period_col in df.columns:
                df["Rank"] = df[last_period_col].rank(ascending=False, method='dense')
            else:
                df["Rank"] = np.nan

            if metric.endswith(GROWTH) or metric.endswith(DELTA):
                max_rank = df["Rank"].max()
                df["Rank"] = df["Rank"].apply(lambda x: -1 if x == max_rank else x)
            df["Rank"] = df["Rank"].astype(str)
            # if there is only one column set the diff_col as nan
            if diff_col not in df.columns:
                df[diff_col] = np.nan
            else:
                df["Diff Rank"] = df[diff_col].rank(ascending=False, method='dense')
                max_diff_rank = df["Diff Rank"].max()
                df["Diff Rank"] = df["Diff Rank"].apply(lambda x: -1 if x == max_diff_rank else x)
                df["Diff Rank"] = df["Diff Rank"].apply(lambda x: f"{first_period_col} to {last_period_col} Difference__{x}")
            # format columns
            if not do_not_format:
                df = df.applymap(lambda x: self.helper.get_formatted_num(x, fmt, pretty_num=True) if isinstance(x, (int, float)) and x != "Rank" else x)

            df["Rank"] = df["Rank"].astype(float)
            df = df.reset_index()
            df.columns.name = None

            dfs.append(df)

        df = pd.concat(dfs, axis=0)

        # remove duplicate summary cols, keep order
        summary_cols = list(dict.fromkeys(summary_cols))

        return df, summary_cols

    # function to handle the interaction between breakout and filters
    # this function is hanlding special tokens
    def process_filters(self, filters, dims, top_n):

        dim_filters, metric_filters = old_split_dim_and_metric_filters(filters, self.dim_props)

        if top_n == 1:
            top_n = 5

        # if compare dim provided, set dims to compare dim, overriding breakout dims
        required_dim_vals = defaultdict(list)  # mapping of dim to list of required dim vals
        for f in dim_filters:
            if f['val'] in [OTHER, COMPETITORS]:
                dims = [f['col']]
                required_dim_vals[dims[0]] = [f_val['val'] for f_val in dim_filters if f_val['col'] == dims[0] and f_val['val'] not in [OTHER, COMPETITORS] and f_val['op'].lower() in ['=', 'in']]
                dim_filters = [f for f in dim_filters if not (f['col'] == dims[0] and f['op'].lower() in ['=', 'in'])]

        # remove all filters with '<other>' or '<competitors>'
        dim_filters = [f for f in dim_filters if f['val'] not in [OTHER, COMPETITORS]]

        # if no dim breakout, get dim with most filters for compare
        if not dims and dim_filters:
            # create filter dict without tokens to count number of filters per dim
            filter_dict = {}
            for f in dim_filters:
                if not is_filter_token(f['val']):
                    if f['col'] not in filter_dict:
                        filter_dict[f['col']] = []
                    filter_dict[f['col']].append(f['val'])

            # set dim to dim with most filters as long as its more than 1 filter for that dim
            compare_dim = max(filter_dict, key=lambda k: len(filter_dict[k]))
            if len(filter_dict[compare_dim]) > 1:
                dims = [compare_dim]

        # if <all> in filters, remove top_n limit, and remove dim from filters
        if dims:
            for f in dim_filters:
                if f['col'] in dims and f['val'] == '<all>':
                    top_n = None
                    dim_filters = [f for f in dim_filters if f['col'] not in dims]

        # get rid of an other all filters
        dim_filters = [f for f in dim_filters if f['val'] != '<all>']

        # if breakout exists, and theres only one filter, remove filter and add it to required_dim_vals
        if dims:
            for dim in dims:
                filters_for_dim = [f for f in dim_filters if f['col'] == dim and f['op'].lower() in ['=', 'in']]
                if len(filters_for_dim) == 1:
                    required_dim_vals[dim].append(dim_filters[0]['val'])
                    dim_filters = [f for f in dim_filters if not (f['col'] == dim and f['op'].lower() in ['=', 'in'])]
                elif filters_for_dim:
                    in_filter = [f for f in dim_filters if (f['col'] == dim and f['op'].lower() in ['=', 'in'])]
                    dim_filters = [f for f in dim_filters if not (f['col'] == dim and f['op'].lower() in ['=', 'in'])]
                    in_filter_vals = [f['val'].lower() for f in in_filter]
                    dim_filters.append({"col": dim, "op": "IN", "val": in_filter_vals})

        required_dim_vals = defaultdict(list, {k: [i.lower() for i in v] for k, v in required_dim_vals.items()})
        dims = [dim for dim in dims if dim not in self.metric_filter_columns]

        filters = dim_filters + metric_filters
        return filters, dims, top_n, required_dim_vals

    # function to get the data for core metrics
    def _run_base(self, metrics, dims, fils, period_filter, table_specific_filters, top_n, top_n_direction, required_dim_vals):
        filters = copy.deepcopy(fils)
        metrics = [self.helper.get_metric_prop(m, self.metric_props) for m in metrics]

        if period_filter:
            filters.append(period_filter)
        additional_filters = []
        for dim in dims:
            additional_filters.extend(table_specific_filters.get(dim, table_specific_filters.get('default', [])))

        df = self.get_trend_data(metrics, dims, filters + additional_filters, top_n, top_n_direction, required_dim_vals)
        if df[~df["date_column"].isnull()].empty:
            raise exit_with_status("Report did not run: Please tell the user there is no data available for the selected set of filter.  Recommend changing the filter criteria or ask a different question to proceed.")

        # rename metric cols
        rename_dict = {m["col"]: m["name"] for m in metrics}
        df['metric'] = df['metric'].apply(lambda x: rename_dict.get(x, x))

        return df

    # function to get the denominator data for market share metrics
    def _run_share(self, base_df, metrics, dims, fils, period_filter, table_specific_filters, top_n, top_n_direction, required_dim_vals):
        filters = copy.deepcopy(fils)
        base_df = base_df.copy()

        # create a dict of share metric properties
        underlying_metric_props = {}
        for metric in metrics:
            metric_dict = self.helper.get_metric_prop(metric, self.metric_props)
            underlying_metric = metric_dict.get("component_metric")

            # check that share metric has underlying metric details. Required for share calculations
            if not underlying_metric:
                raise exit_with_status(f"Component metric for '{metric}' not specified in metric properties")

            underlying_metric_dict = self.helper.get_metric_prop(underlying_metric, self.metric_props)

            underlying_metric_props[metric] = underlying_metric_dict

        # metric columns to select from data
        share_metric_props = [underlying_metric_props[m] for m in metrics]

        if period_filter:
            filters.append(period_filter)

        additional_filters = []
        for dim in dims:
            additional_filters.extend(table_specific_filters.get(dim, table_specific_filters.get('default', [])))

        # get share base
        join_cols = ['date_column', self.date_alias, 'metric']
        if self.date_sort_col:
            join_cols.append(self.pandas_sort_col)

        # rename metrics from component metric column to component metric
        rename_dict = {underlying_metric_props[m]['col']:
                           underlying_metric_props[m]['name'] for m in metrics}

        if dims:
            dim_dfs = []

            for dim in dims:

                breakout_label = self.helper.get_dimension_prop(dim, self.dim_props).get("label", dim)

                dim_required_vals = defaultdict(list)
                base_dim_df = base_df[base_df['dim'] == dim]

                # check if any of the filters are in owner hierarchy, remove them from market_filters
                market_filters = [f for f in filters if f['col'] not in self.dim_hier.owner_cols]

                # check if filters are the same for numerator and denominator
                # if it's the same then calculate contribution to total
                if market_filters == filters:
                    groupby = []
                    dim_join_cols = join_cols
                    self.contributions.append(breakout_label)
                # only breakout the denominator if it is not an owner column
                elif dim in self.dim_hier.owner_cols:
                    groupby = []
                    dim_join_cols = join_cols
                    self.contributions.append(breakout_label)
                else:
                    groupby = [dim]
                    self.share_within.append(breakout_label)
                    dim_join_cols = join_cols + ['dim', 'dim_val']
                    # get the dim_vals needed for denominator
                    dim_vals = list(base_dim_df['dim_val'].unique())
                    # dim_required_vals[dim] = [d.lower() for d in dim_vals]

                dim_market_df = self.get_trend_data(share_metric_props, groupby, market_filters + additional_filters, top_n, top_n_direction, dim_required_vals)

                dim_market_df['metric'] = dim_market_df['metric'].apply(lambda x: rename_dict.get(x, x))
                dim_market_df = dim_market_df.rename(columns={'value': 'market_value'})

                # join the market_df with the base_df
                base_dim_df = pd.merge(base_dim_df, dim_market_df, on=dim_join_cols, how='inner')

                dim_dfs.append(base_dim_df)
            market_df = pd.concat(dim_dfs, ignore_index=True)
        else:
            # check if any of the filters are in owner hierarchy, remove them from market_filters
            market_filters = [f for f in filters if f['col'] not in self.dim_hier.owner_cols]
            total_market_df = self.get_trend_data(share_metric_props, [], market_filters + additional_filters, top_n, top_n_direction, required_dim_vals)

            total_market_df['metric'] = total_market_df['metric'].apply(lambda x: rename_dict.get(x, x))
            # calculate market share
            total_market_df = total_market_df.rename(columns={'value': 'market_value'})

            market_df = pd.merge(base_df, total_market_df, on=join_cols, how='inner')

        # calculate market share
        market_df['market_share'] = market_df['value'] / market_df['market_value']
        dim_cols = ['dim_val', 'dim'] if dims else []
        market_df = market_df[[*join_cols, 'market_share'] + dim_cols]
        market_df = market_df.rename(columns={'market_share': 'value'})

        # rename metrics from component metric to share metric
        rename_dict = {underlying_metric_props[m]['name']: m for m in metrics}
        market_df['metric'] = market_df['metric'].apply(lambda x: rename_dict.get(x, x))

        return market_df

    # this function is to create the chart in different tabs
    def get_display_charts(self, viz_func=None):

        if viz_func:
            for name in self.display_charts.keys():
                df = self.display_charts[name]['df']
                viz_func(df, name)
                del self.display_charts[name]['df']
        else:
            if len(self.display_charts) == 0:
                return None
            return self.display_charts[list(self.display_charts.keys())[0]]['df']

    # this function is to create the table
    def get_display_tables(self, viz_func=None):

        if viz_func:
            for metric, df in self.display_dfs.items():
                viz_func(df, metric)
        else:
            if len(self.display_dfs) == 0:
                return None
            return self.display_dfs[list(self.display_dfs.keys())[0]]

    # function to cap how many charts we will be creating
    # since we are creating chart for each metric and dim combination, this is added as a fail safe mechanism to avoid creating too many charts
    def limit_data_pull(self, metrics, dims):

        final_metrics = []
        final_dims = []

        for metric in metrics:

            # check if adding a metric will exceed combination limit of metrics and dims
            if (len(final_metrics) + 1) * len(final_dims) > self.max_num_charts:
                break

            final_metrics.append(metric)

            # only add dims once
            if not final_dims:

                for dim in dims:

                    # check if adding a dim will exceed combination limit of metrics and dims
                    if len(final_metrics) * (len(final_dims) + 1) > self.max_num_charts:
                        break

                    final_dims.append(dim)

        # give this information to the LLM
        if len(final_metrics) != len(metrics):
            self.notes.append(f"Analysis was run using only the following metrics: {final_metrics}")

        if len(final_dims) != len(dims):
            self.notes.append(f"Analysis was run using only the following breakouts: {final_dims}")

        return final_metrics, final_dims

    def _update_metrics_and_props(self, metrics, metric_props):
        return metrics, metric_props

    # wrapper around run function that gets the parameter from env object
    def _run_from_env(self):

        if self.env is None:
            raise exit_with_status("self.env is required")

        self.sp = self.env.sp

        self.hide_totals = self.env.trend_parameters.get("hide_totals", False)
        self.time_granularity = self.env.trend_parameters.get("time_granularity")
        self.dataset_min_date = self.env.trend_parameters.get("dataset_min_date")
        self.dataset_max_date = self.env.trend_parameters.get("dataset_max_date")

        df = self.run(
            metrics=self.env.trend_parameters.get("metrics"),  # trend skill can handle any number of metrics, but must have at least 1
            dims=self.env.trend_parameters.get("breakouts"),  # optional dim breakouts
            filters=self.env.trend_parameters.get("query_filters"),  # dim filters to run the analysis on
            period_filter=self.env.trend_parameters.get("period_filter"),  # period to run the analysis on
            top_n=self.env.trend_parameters.get("limit_n", 10),  # limit number of dim breakout values
            metric_props=self.env.metric_props,
            dim_props=self.env.dim_props,
            view=self.env.trend_parameters.get("derived_sql_table"),
            date_labels=self.env.trend_parameters.get("date_labels"),
            metric_name_column=self.env.trend_parameters.get("metric_name_column"),
            metric_filter_columns=self.env.trend_parameters.get("metric_filter_columns")
        )

        # handle parameter bubble updates after running analysis
        if self.top_n:
            self.paramater_display_infomation["limit_n"] = f"Top {str(self.top_n)}"

        # set the viz header
        self.env.viz__header = self.viz_header

        return df

    def run_from_env(self):
        return self._run_from_env()

    def get_metrics(self, metrics, filters):
        if not metrics:
            raise exit_with_status(f"Ask user to specify a valid metric. Please do not make choice on user's behalf.")
        self.value_metric = None
        if self.metric_name_column:
            self.value_metric = self.helper.get_metric_prop(metrics[0].lower(), self.metric_props)
            metric_filters = [f for f in filters if f['col'] in self.metric_filter_columns]
            metric_df = self.pull_data_func(filters=metric_filters, breakouts=[self.metric_name_column])
            metrics = list(metric_df[self.metric_name_column].unique())
        metrics = [self.helper.get_metric_prop(m, self.metric_props).get('name') for m in metrics]
        return metrics

    # main function that runs the trend analysis
    def run(self, metrics, dims, filters, period_filter, view="", table_specific_filters={}, top_n=10, top_n_direction="top",
            tie_breaker="dense", date_labels={}, metric_props={}, dim_props={}, metric_name_column=None, metric_filter_columns=[]):
        self.view = view

        top_n_direction = "DESC" if top_n_direction == "top" else "ASC"
        target_metrics = copy.copy(metrics)
        # allow for single metric and dim or list of metrics and dims
        if isinstance(metrics, str):
            metrics = [metrics]
        if isinstance(dims, str):
            dims = [dims]
        if isinstance(top_n, str):
            top_n = int(top_n)

        self.metric_name_column = metric_name_column
        self.metric_filter_columns = metric_filter_columns
        self.metric_props = metric_props
        self.dim_props = dim_props

        # update metric props, this does nothing by default but gives the ability to update metric props for overrides
        metrics, self.metric_props = self._update_metrics_and_props(metrics, metric_props)

        metrics = self.get_metrics(metrics, filters)

        self.check_required_params(metrics, metric_props)
        metrics, dims = self.limit_data_pull(metrics, dims)

        # process special token filters
        filters, dims, top_n, required_dim_vals = self.process_filters(filters, dims, top_n)

        # group metrics by share vs non-share
        trend_metrics = {
            "share": [],
            "base": []
        }
        for metric in metrics:
            metric_dict = self.helper.get_metric_prop(metric, self.metric_props)
            is_share = metric_dict.get("metric_type") == "share"
            if is_share:
                trend_metrics["share"].append(metric)
                base_metric = metric_dict.get("component_metric")
                trend_metrics["base"].append(base_metric)
            else:
                trend_metrics["base"].append(metric)

        # remove any duplicates from trend_metrics
        trend_metrics["share"] = list(dict.fromkeys(trend_metrics["share"]))
        trend_metrics["base"] = list(dict.fromkeys(trend_metrics["base"]))

        # trend guardrails for trend analysis
        if len(trend_metrics["share"]) > 0:
            if not self.dim_hier.dim_hierarchy:
                raise exit_with_status("Ask user to reach out to admin to setup a dimension hierarchy on dataset to run trend share analysis.")

        # run base and share trend queries
        trend_args = [dims, filters, period_filter, table_specific_filters, top_n, top_n_direction, required_dim_vals]

        base_df = self._run_base(trend_metrics['base'], *trend_args)
        share_df = self._run_share(base_df, trend_metrics["share"], *trend_args) if trend_metrics["share"] else pd.DataFrame()

        denom_filters = old_get_filters_headline([f for f in filters if f['col'] not in self.dim_hier.owner_cols], headline_seperator=", ", metric_props=metric_props, dim_props=dim_props) or "Total"
        num_filters = old_get_filters_headline([f for f in filters if f['col'] in self.dim_hier.owner_cols], headline_seperator=", ", metric_props=metric_props, dim_props=dim_props)

        # get lowest level of owner filter for footer
        owner_filters = [f for f in filters if f['col'] in self.dim_hier.owner_cols]
        if owner_filters:
            lowerst_ms_filter = self.dim_hier.get_lowest_level_ms_dim_filter(owner_filters)
            lowest_ms_dim = lowerst_ms_filter.get("col")
            dim_col_label_map = {m.get("sql"): m.get("label") for k, m in self.dim_props.items()}
            self.lowest_ms_dim_label = dim_col_label_map.get(lowest_ms_dim, lowest_ms_dim)
        else:
            self.lowerst_ms_filter = "dimension members"

        def get_share_title(num_filters, denom_filters, sep="•"):

            if num_filters != "":
                return num_filters + " | " + denom_filters if denom_filters is not None else "All Dataset"
            else:
                return denom_filters if denom_filters is not None else "All Dataset"

        if not share_df.empty:
            df = pd.concat([base_df, share_df], ignore_index=True)

            title = get_share_title(num_filters, denom_filters)
        else:
            df = base_df
            title = old_get_filters_headline(filters, headline_seperator=", ", metric_props=metric_props, dim_props=dim_props) or "Total"
        df, metrics = self.get_growth_data(df, metrics)

        # sort dfs
        sort_cols = [self.pandas_sort_col] if not dims else [self.pandas_sort_col, 'dim', 'dim_val']
        df = df.sort_values(by=sort_cols)

        # guardrail for trend periods, must be more than 1
        periods_in_data = list(df[self.date_alias].unique())
        periods_in_data = [p for p in periods_in_data if p != self.total_col]
        if len(periods_in_data) < 2:
            msgs = [f"Trend analysis is meaningful and actionable for more than one period to analyze. The date range provided has {len(periods_in_data)} {self.time_granularity}."]
            if len(periods_in_data) > 0:
                msgs.append(f"Even though we have {len(periods_in_data)} it's not meaningful for trend analysis. Hence we cannot proceed with the analysis with given date range.")
            msgs.append("Please ask user to provide or update the start and end dates for trend analysis to be meaningful and actionable, along with the desired time interval (e.g., yearly, monthly) for the breakdown.")
            if self.dataset_min_date and self.dataset_max_date:
                data_start_date = self.dataset_min_date.strftime("%Y-%m-%d")
                data_end_date = self.dataset_max_date.strftime("%Y-%m-%d")
                msgs.append(f"Please inform user that data is available from {data_start_date} to {data_end_date}. Please make sure you present the data in the appropriate time interval.")
            msgs.append("Please make sure user understands the importance of having more than one period to analyze and has information about data availability.")
            msgs.append("Please do not make choice on user's behalf.")
            raise exit_with_status(" ".join(msgs))

        # fact ONLY metrics
        filtered_target_metrics = [f"{metric}{GROWTH}" for metric in target_metrics] + \
                                  [f"{metric}{DELTA}" for metric in target_metrics] + \
                                  [f"{metric}{VARIANCE_DELTA}" for metric in target_metrics] + \
                                  [f"{metric}{VARIANCE_GROWTH}" for metric in target_metrics] + \
                                  target_metrics

        # Filter the DataFrame based on the updated target metrics
        filtered_target_metrics = [str(metric).lower() for metric in filtered_target_metrics]
        filtered_df_for_facts = df[df['metric'].str.lower().isin(filtered_target_metrics)]

        # viz df
        viz_table, _ = self._get_facts_df(df)
        # trend facts
        all_facts, summary_cols = self.get_facts_df(filtered_df_for_facts)
        # top 3 change
        self.top_facts, _ = self.get_facts_df(filtered_df_for_facts, top_growth=True, asc=False)
        # bottom 3 change
        self.bottom_facts, _ = self.get_facts_df(filtered_df_for_facts, top_growth=True, asc=True)

        # create summarized table for final facts
        self.facts = all_facts[[sc for sc in summary_cols if sc in all_facts.columns]]

        # remove special calc cols from display dfs
        remove_cols = ["Rank", "Diff Rank", "Highest Value", "Lowest Value"]
        for col in remove_cols:
            if col in viz_table.columns:
                viz_table.drop(columns=[col], inplace=True)

        # setup table
        viz_table.rename(columns={"metric": "Metric", "dim": "Breakout", "dim_val": "Name"}, inplace=True)

        self.display_dfs = {
            'Metrics Table': viz_table
        }
        self.default_table_template = self.get_default_table_jinja()
        self.default_table = self.get_default_table(viz_table)

        date_str = old_get_date_label_str(date_labels) if date_labels else ""
        self.footnotes = {}
        # add details about share within and contributions to subheadline
        if self.share_within or self.contributions:
            # add footnotes for share within and contributions by dim, this is being referenced in the viz
            self.footnotes = self.get_footer(num_filters, denom_filters, self.share_within, self.contributions)
            self.share_within = self.helper.and_comma_join(self.share_within)
            self.contributions = self.helper.and_comma_join(self.contributions)
            if self.share_within and self.contributions:
                subtitle = f"Trend analysis by {self.date_alias}, within {self.share_within}, breakout by {self.contributions}{date_str}"
            elif self.share_within:
                subtitle = f"Trend analysis by {self.date_alias} within {self.share_within}{date_str}"
            else:
                subtitle = f"Trend analysis by {self.date_alias}, breakout by {self.contributions}{date_str}"
        else:
            subtitle = f"Trend by {self.date_alias}{date_str}"

        self.viz_header = get_viz_header(title=title, subtitle=subtitle, warning_messages=self.get_row_limit_warning_message())
        self.warning_message = self.get_row_limit_warning_message()

        self.title = title
        self.subtitle = subtitle

        # setup charts

        # filter to specified metrics, don't include component metrics unless specified. don't include total columns
        chart_df = df[(df['metric'].isin(metrics)) & (~df["date_column"].isnull())]
        # TODO: Changing the get_charts function
        self.display_charts = self.get_charts(chart_df, metrics, dims)
        self.default_chart_template = self.get_default_chart_jinja()
        self.default_chart = self.get_default_chart_jinja(render_jinja=True)  # keeping around for backwards compatibility, renders the first chart

        self.notes.append(self.get_analysis_message(metrics, dims, filters, date_labels, share_title=get_share_title(num_filters, denom_filters, sep="and") if not share_df.empty else ""))
        date_notes = []
        date_notes.append(f"The analysis includes three main components: a chart, a table, and a textual analysis.")
        date_notes.append(f"The key facts section highlights only key points or changes and does not represent the full {self.time_granularity}ly data available on chart and table.")
        date_notes.append(f"The data presented in the charts and tables covers the {self.time_granularity}ly data from {self.actual_first_period} to {self.actual_last_period} only.")
        if self.hide_totals:
            date_notes.append(f"There is no analysis aggregation for the full period.")
        self.notes.append(" ".join(date_notes))
        self.df_notes = pd.DataFrame({"Note to the assistant:": self.notes})

        # updated params, need for updating chat pills
        self.top_n = top_n if dims else None

        return df

    def get_footer(self, num_filters, denom_filters, share_within, contributions):
        """Adding footer for each dimension tab on how the share is calculated"""
        footnotes = {}
        if denom_filters == "Total":
            filter_str = ""
        else:
            filter_str = f" in the {denom_filters}"

        for dim in share_within:
            footnotes[dim] = f"{num_filters}'s share within each {dim} is calculated as its performance within that {dim} divided by the total performance of all {self.lowest_ms_dim_label} in that {dim}{filter_str}."
        for dim in contributions:
            footnotes[dim] = f"{num_filters} share by {dim} is calculated as the performance of each {dim} divided by the total performance of all {dim}{filter_str}."
        return footnotes


class TrendTemplateParameterSetup(TemplateParameterSetup):

    def __init__(self, sp=None, env=None):
        if sp is None:
            sp = SkillPlatform()

        super().__init__(sp=sp)
        self.map_env_values(env=env)

        # set the skill platform on env
        env.sp = sp
        

    """
    Creates parameters necessary to run the trend skill from the copilot skill parameter values and dataset.
    These are set on the env under trend_parameters, ie env.trend_parameters.

    Adds UI bubbles for certain parameters.
    """

    def map_env_values(self, env=None):

        """
        This function currently hardcodes the following names of the copilot skill variables in the templates, specifially
        - metrics
        - breakouts
        - time_granularity
        - limit_n
        - date_col
        TODO: Create way to reference these dynamically from the copilot skill.
        """

        if env is None:
            raise exit_with_status("env namespace is required.")

        trend_parameters = {}
        pills = {}

        ## Setup DB

        database_id = self.dataset_metadata.get("database_id")
        trend_parameters["table"] = self.dataset_metadata.get("sql_table")
        trend_parameters["derived_sql_table"] = self.dataset_metadata.get("derived_table_sql") or ""
        trend_parameters["dataset_min_date"] = self.dataset_metadata.get("min_date")
        trend_parameters["dataset_max_date"] = self.dataset_metadata.get("max_date")

        trend_parameters["con"] = Connector("db", database_id=database_id, sql_dialect=self.dataset_metadata.get("sql_dialect"), limit=self.sql_row_limit)

        _, trend_parameters["dim_hierarchy"] = self.sp.data.get_dimension_hierarchy()
        trend_parameters["constrained_values"] = self.constrained_values

        ## Map Env Variables

        # Get metric_props, dim_props, setting on env since the chart templates reference these
        misc_metric_props = self.misc_info.get("metric_props", {})
        if misc_metric_props:
            env.metric_props = misc_metric_props
        else:
            env.metric_props = self.get_metric_props()

        env.dim_props = self.get_dimension_props()

        metric_name_column = self.misc_info.get("metric_name_column")
        metric_filter_columns = self.misc_info.get("metric_filter_columns", [])
        trend_parameters["metric_name_column"] = metric_name_column
        trend_parameters["metric_filter_columns"] = metric_filter_columns

        # Get metrics and metric pills
        trend_parameters["metrics"] = env.metrics
        metric_pills = self.get_metric_pills(env.metrics, env.metric_props)

        # Get filters by dimension
        trend_parameters["query_filters"], query_filters_pills = self.parse_dimensions(env, allowed_tokens=['<other>', '<competitors>', '<all>'])

        # Parse breakout dims to the sql columns
        trend_parameters["breakouts"], breakout_pills = self.parse_breakout_dims(env.breakouts)

        # Period Handling
        default_granularity = self.dataset_metadata.get("default_granularity")
        time_granularity = env.time_granularity or default_granularity
        periods = env.periods
        trend_parameters['is_period_table'] = self.is_period_table

        # guardrails for unsupported calculated filters
        calculated_metric_filters = env.calculated_metric_filters if hasattr(env, "calculated_metric_filters") else None
        query, llm_notes, _, _ = self.get_metric_computation_filters(env.metrics, calculated_metric_filters, "None", env.metric_props)
        if query:
            self.get_unsupported_filter_message(llm_notes, 'trend analysis')

        if not self.is_period_table:
            start_date, end_date = self.handle_periods(periods, allowed_tokens=['<no_period_provided>', '<since_launch>'])
            date_labels = {"start_date": start_date, "end_date": end_date}

            # format date labels
            for key, value in date_labels.items():
                date_labels[key] = self.helper.format_date_from_time_granularity(value, default_granularity)

            # date/period column metadata. Assumes the date column is a date type
            period_col = env.date_col if hasattr(env, "date_col") and env.date_col else self.get_period_col()

            if not period_col:
                exit_with_status("A date column must be provided.")

            # create period filter using start date and end date
            if start_date and end_date:
                period_filter = {"col": period_col, "op": "BETWEEN", "val": f"'{start_date}' AND '{end_date}'"}
            else:
                period_filter = {}

            # required for trend skill to enable aggregating at different time granularities
            time_obj = self.get_trend_time_obj(period_col, "", time_granularity)
        else:
            date_filters, time_granularity = self.get_time_variables(periods)
            period_filters, date_labels = self.get_period_filters(sql_con=trend_parameters["con"],
                                                                  date_filters=date_filters,
                                                                  growth_type=None)
            if period_filters:
                period_filter = period_filters[0]
            else:
                period_filter = {}
            start_date = date_labels.get("start_date")
            end_date = date_labels.get("end_date")
            time_obj = {"col": time_granularity, "name": time_granularity, "sort_col": f"{time_granularity}{DATE_SEQ_COL_SUFFIX}"}
            trend_parameters["period_df"] = self.date_df

        # get periods in year
        if isinstance(self.periods_in_year, dict):
            trend_parameters["periods_in_year"] = self.periods_in_year
        else:
            trend_parameters["periods_in_year"] = None

        trend_parameters["date_labels"] = date_labels

        # Set the trend date parameters

        trend_parameters["period_filter"] = period_filter
        trend_parameters["time_obj"] = time_obj
        trend_parameters["time_granularity"] = time_granularity

        # convert limit_n to an int
        if hasattr(env, "limit_n") and env.limit_n:
            if env.limit_n == NO_LIMIT_N:
                trend_parameters["limit_n"] = None
            else:
                trend_parameters["limit_n"] = self.convert_to_int(env.limit_n)

        ## add UI bubbles

        if metric_pills:
            pills["metrics"] = f"Metrics: {self.helper.and_comma_join(metric_pills)}"
        if query_filters_pills:
            pills["filters"] = f"Filter: {self.helper.and_comma_join(query_filters_pills)}"
        if breakout_pills:
            pills["breakouts"] = f"Breakouts: {self.helper.and_comma_join(breakout_pills)}"
        if start_date and end_date:
            if start_date == end_date:
                pills["period"] = f"Period: {start_date}"
            else:
                pills["period"] = f"Period: {start_date} to {end_date}"
        if time_granularity:
            pills["time_granularity"] = f"Time Granularity: {str(time_granularity)}"
        if hasattr(env, "growth_type"):
            if str(env.growth_type).lower() in ["p/p", "y/y"]:
                pills["growth_type"] = f"Growth: {str(env.growth_type)}"
            elif str(env.growth_type).lower() in VARIANCE_TYPES:
                pills["growth_type"] = f"Variance: {str(env.growth_type)}"

        trend_parameters["ParameterDisplayDescription"] = pills

        # TODO: Adding axis grouping rules to trend_parameters
        trend_parameters["axis_grouping_rules"] = self.dataset_metadata.get("misc_info", {}).get("axis_grouping")

        ## Set the trend parameters
        env.trend_parameters = trend_parameters


# advance trend class, inherits from trend analysis but adds additional functionality by overriding some methods
class AdvanceTrend(TrendAnalysis):
    def __init__(self, table: str, sql_exec: Connector, time: dict, dim_hierarchy: dict = {}, constrained_values={}, max_num_charts=10, df_provider=None):
        super().__init__(table=table,
                         sql_exec=sql_exec,
                         time=time,
                         dim_hierarchy=dim_hierarchy,
                         constrained_values=constrained_values,
                         max_num_charts=max_num_charts,
                         df_provider=df_provider)

        self.warning_messages = []
    def get_row_limit_warning_message(self):
        messages = self.warning_messages
        if self.hit_row_limit:
            messages.append(f"The following analysis has been limited to {self.helper.get_formatted_num(self.sql_exec.limit, ',.0f')} rows which may impact the accuracy of the observations made.")

        message = " ".join(messages)
        if message:
            message = f"⚠ {message}"
        return message
    
    def growth_type_label(self, growth_type):
        if str(growth_type).lower() in ["p/p", "y/y"]:
            return "YoY" if str(growth_type).lower() == "y/y" else "PoP" if str(growth_type).lower() == "p/p" else None
        elif str(growth_type).lower() in VARIANCE_TYPES:
            return f"vs. {self.get_variance_type(growth_type).title()}"
        else:
            return None
        
    def get_variance_type(self, growth_type):
        if str(growth_type).lower() in VARIANCE_TYPES:
            return str(growth_type).lower().replace("vs.", "").strip()
        else:
            return None

    def run_from_env(self):
        if self.env is None:
            raise exit_with_status("self.env is required")

        self.sp = self.env.sp

        # Variance runs will not be considered growth runs
        if str(self.env.growth_type).lower() in ["p/p", "y/y"]:
            self.abs_default_series = False
            is_growth_run = True
        else:
            self.abs_default_series = True
            is_growth_run = False

        self.growth_label = self.growth_type_label(self.env.growth_type)
        self.hide_totals = self.env.trend_parameters.get("hide_totals", False) if not is_growth_run else True
        self.periods_in_year = self.env.trend_parameters.get("periods_in_year")
        self.core_period = copy.deepcopy(self.env.trend_parameters.get("period_filter"))
        period_df = self.env.trend_parameters.get("period_df")
        self.time_granularity = self.env.trend_parameters.get("time_granularity")
        self.start_date, self.end_date = None, None
        self.start_seq, self.end_seq = None, None
        self.is_period_table = self.env.trend_parameters.get("is_period_table", False)
        self.dataset_min_date = self.env.trend_parameters.get("dataset_min_date")
        self.dataset_max_date = self.env.trend_parameters.get("dataset_max_date")
        self.paramater_display_infomation = self.env.trend_parameters.get("ParameterDisplayDescription", {})

        # this code block is updating the period filter for growth analysis since we need to pull more data to calculate the growth
        if self.core_period and is_growth_run:
            col = self.core_period.get("col")
            op = self.core_period.get("op")
            values = self.core_period.get("val")
            time_granularity = self.env.trend_parameters.get("time_granularity")
            if self.is_period_table:
                dim = col.replace(DATE_SEQ_COL_SUFFIX, "")

                if dim not in self.periods_in_year:
                    raise exit_with_status("Periods in year is required for sequence based growth analysis. Ask user to reach out to admin to setup periods in year.")
                # destructure sequence values
                if op == "=":
                    start_seq = int(values)
                    end_seq = start_seq
                else:
                    start_seq, end_seq = values.split(" AND ")
                    start_seq, end_seq = int(start_seq), int(end_seq)

                periods_in_year_for_dim = self.periods_in_year.get(dim)

                # update start sequence
                if str(self.env.growth_type).lower() == "p/p":
                    updated_seq = start_seq - int(12 / periods_in_year_for_dim)
                else:
                    updated_seq = start_seq - int(periods_in_year_for_dim)

                # update filter
                if updated_seq < 1:
                    updated_seq = 1
                period_filter = {"col": col, "op": "BETWEEN", "val": f"{updated_seq} AND {end_seq}"}

                # get filter in breakout granularity
                filtered_period_df = period_df[(period_df[col] >= start_seq) & (period_df[col] <= end_seq)][[f"{time_granularity}{DATE_SEQ_COL_SUFFIX}"]].astype(int)
                self.start_seq, self.end_seq = filtered_period_df[f"{time_granularity}{DATE_SEQ_COL_SUFFIX}"].min(), filtered_period_df[f"{time_granularity}{DATE_SEQ_COL_SUFFIX}"].max()

            else:
                start_date, end_date = values.split(" AND ")
                start_date, end_date = start_date.replace("'", ""), end_date.replace("'", "")
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                print(f"time: {time_granularity}")
                if str(self.env.growth_type).lower() == "p/p":
                    if time_granularity == "month":
                        new_start_date = start_date_obj - relativedelta(months=1)
                    elif time_granularity == "year":
                        new_start_date = start_date_obj - relativedelta(years=1)
                    elif time_granularity == "week":
                        new_start_date = start_date_obj - relativedelta(weeks=1)
                    elif time_granularity in ["day", "date"]:
                        new_start_date = start_date_obj - relativedelta(days=1)
                    elif time_granularity == "quarter":
                        new_start_date = start_date_obj - relativedelta(months=3)
                    else:
                        exit_with_status(f"Unsupported time granularity for growth: {time_granularity}")
                else:
                    new_start_date = start_date_obj - relativedelta(years=1)
                period_filter = {"col": col, "op": "BETWEEN", "val": f"""'{new_start_date.strftime("%Y-%m-%d")}' AND '{end_date}'"""}
                self.start_date, self.end_date = start_date, end_date
        else:
            period_filter = self.core_period
        print(f"Updated Period Filter: {period_filter}")
        df = self.run(
            metrics=self.env.trend_parameters.get("metrics"),  # trend skill can handle any number of metrics, but must have at least 1
            dims=self.env.trend_parameters.get("breakouts"),  # optional dim breakouts
            filters=self.env.trend_parameters.get("query_filters"),  # dim filters to run the analysis on
            period_filter=period_filter,  # period to run the analysis on
            top_n=self.env.trend_parameters.get("limit_n", 10),  # limit number of dim breakout values
            metric_props=self.env.metric_props,
            dim_props=self.env.dim_props,
            view=self.env.trend_parameters.get("derived_sql_table"),
            date_labels=self.env.trend_parameters.get("date_labels"),
            metric_name_column=self.env.trend_parameters.get("metric_name_column"),
            metric_filter_columns=self.env.trend_parameters.get("metric_filter_columns")
        )

        # handle parameter bubble updates after running analysis
        if self.top_n:
            self.paramater_display_infomation["limit_n"] = f"Top {str(self.top_n)}"

        # set the viz header
        self.env.viz__header = self.viz_header

        return df

    # adding metric props for growth metrics
    def _update_metrics_and_props(self, metrics, metric_props):
        if str(self.env.growth_type).lower() in ["p/p", "y/y"]:
            unchanged_props = copy.deepcopy(metric_props)
            for metric_name, metric_info in unchanged_props.items():
                met_info = copy.deepcopy(metric_info)
                delta_info = copy.deepcopy(metric_info)
                growth_name = f"{metric_name}{GROWTH}"
                delta_name = f"{metric_name}{DELTA}"
                met_info['name'] = growth_name
                delta_info['name'] = delta_name
                is_delta_growth = met_info.get('hide_percentage_change')
                # difference will have same format as original metric
                delta_info['fmt'] = met_info['fmt']
                # override growth metric format
                met_info['fmt'] = met_info.get('growth_fmt')
                delta_info['label'] = f"{met_info['label'] or met_info['name']} {self.growth_label} Difference"
                if is_delta_growth:
                    met_info['label'] = f"{met_info['label'] or met_info['name']} {self.growth_label} Growth"
                    # if delta growth then update the format
                    delta_info['fmt'] = met_info['fmt']
                else:
                    met_info['label'] = f"{met_info['label'] or met_info['name']} {self.growth_label} % Change"
                if met_info.get('component_metric'):
                    met_info['component_metric'] = f"{met_info['component_metric']}{GROWTH}"
                metric_props[growth_name] = met_info
                metric_props[delta_name] = delta_info
        elif str(self.env.growth_type).lower() in VARIANCE_TYPES:

            variance_type = self.get_variance_type(self.env.growth_type)
            unchanged_props = copy.deepcopy(metric_props)

            metrics = [m.lower() for m in metrics]

            variance_metrics = []
            for metric in metrics:

                # if metric is already in variance_metrics, skip since it's already added
                if metric in variance_metrics:
                    continue

                if metric.endswith(variance_type):
                    variance_metrics.append(metric)
                else:
                    variance_metrics.append(f"{metric}_{variance_type}")

            for var_metric in variance_metrics:
                if var_metric.lower() not in metric_props:
                    raise exit_with_status(f"Metric {var_metric} not found in metric_props")
                
                component_metric = var_metric.replace(f"_{variance_type}", "")
                
                ## ensure the variance metric and component metric are added to the metrics list
                if var_metric not in metrics:
                    metrics.append(var_metric)

                if component_metric not in metrics:
                    metrics.append(component_metric)

                ## update metric props for variance differnce and variance percent difference metrics 
                variance_difference_metric = f"{var_metric}{VARIANCE_DELTA}"
                variance_growth_metric = f"{var_metric}{VARIANCE_GROWTH}"

                met_info = self.helper.get_metric_prop(component_metric, metric_props)
                component_metric_label = self.helper.get_metric_prop(component_metric, metric_props).get('label', component_metric)
                is_delta_growth = met_info.get('hide_percentage_change')

                variance_difference_met_info = copy.deepcopy(met_info)
                variance_growth_info = copy.deepcopy(met_info)

                variance_difference_met_info['name'] = variance_difference_metric
                variance_difference_met_info['label'] = f"Trend of {component_metric_label} Variance from {variance_type.title()}"

                variance_growth_info['name'] = variance_growth_metric
                if is_delta_growth:
                    variance_growth_info['label'] = f"Trend of {component_metric_label} Variance Growth from {variance_type.title()}"
                    variance_growth_info['fmt'] = met_info['fmt']
                else:
                    variance_growth_info['label'] = f"Trend of {component_metric_label} Variance % from {variance_type.title()}"
                    variance_growth_info['fmt'] = met_info['growth_fmt']

                metric_props[variance_difference_metric] = variance_difference_met_info
                metric_props[variance_growth_metric] = variance_growth_info
                
        return metrics, metric_props

    def generate_date_dataframe(self, start_date, end_date, frequency):
        # Generate all dates between start_date and end_date
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')

        # Initialize the formatted dates list
        all_formatted_dates = []

        # Format dates based on the given frequency
        if frequency == 'month':
            all_formatted_dates = all_dates.strftime('%b %Y')
        elif frequency == 'year':
            all_formatted_dates = all_dates.strftime('%Y')
        elif frequency == 'quarter':
            all_formatted_dates = all_dates.to_period('Q').strftime('Q%q %Y')
        elif frequency == 'week':
            all_formatted_dates = all_dates.to_period('W').strftime('WK%U %Y')
        elif frequency == 'date':
            all_formatted_dates = all_dates.strftime('%Y-%m-%d')
        else:
            raise ValueError("Unsupported frequency. Supported frequencies are 'quarter', 'year', 'month', 'week', 'date'.")

        # Create the DataFrame
        df = pd.DataFrame({
            self.pandas_sort_col: all_dates.strftime('%Y-%m-%d'),
            self.date_alias: all_formatted_dates
        })

        return df

    def get_missing_dates_df(self, df, frequency):
        data_df = df.copy()
        data_df[self.pandas_sort_col] = pd.to_datetime(data_df[self.pandas_sort_col])
        start, end = data_df[self.pandas_sort_col].min(), data_df[self.pandas_sort_col].max()
        all_dates_df = self.generate_date_dataframe(start, end, frequency)
        all_dates_df[self.pandas_sort_col] = pd.to_datetime(all_dates_df[self.pandas_sort_col])
        # this merge will have all dates available in data
        data_dates = pd.merge(data_df, all_dates_df, on=self.pandas_sort_col, how='left', suffixes=('', '__all'))
        # all the formatted dates that are available in data
        available_dates = data_dates[~pd.isnull(data_dates[f"{self.date_alias}__all"])][f"{self.date_alias}__all"].unique()
        # all the formatted dates that are missing in data
        missing_dates = all_dates_df[~all_dates_df[self.date_alias].isin(available_dates)]
        if len(missing_dates) > 0:
            # get one row per missing formatted date with max date as agg
            missing_dates = missing_dates.groupby(self.date_alias).agg({self.pandas_sort_col: 'max'}).reset_index()
            missing_df = missing_dates[[self.pandas_sort_col, self.date_alias]]
            missing_df = missing_df.sort_values(by=self.pandas_sort_col)
            print("********* injected missing dates *********")
            print(missing_df.to_string())
            print("********* end missing dates *********")
        else:
            missing_df = pd.DataFrame()
        return missing_df

    def get_growth_data(self, df, metrics):
        # get list of metrics that are % metrics and non-% metrics
        delta_growth = []
        percentage_growth = []
        for metric_name, metric_info in self.metric_props.items():
            if metric_info.get('hide_percentage_change'):
                delta_growth.append(metric_info.get('name'))
            else:
                percentage_growth.append(metric_info.get('name'))
        # calculate growth
        growth_type = str(self.env.growth_type).lower()
        if growth_type in ["p/p", "y/y"]:
            growth_df = copy.deepcopy(df)

            # if it's normal calender based growth then check if all data is available between first and last date
            # in normal calender implementation pandas_sort_col will be date_column
            if not self.is_period_table and len(growth_df) > 0:
                missing_dates = self.get_missing_dates_df(growth_df, self.time_granularity)
                if len(missing_dates) > 0:
                    # inject missing dates into the growth data, only add date column and formatted date column
                    growth_df = pd.concat([growth_df, missing_dates], ignore_index=True)
                    growth_df = growth_df.sort_values(by=self.pandas_sort_col)

            calendar_sort_df = growth_df[[self.pandas_sort_col]].drop_duplicates().sort_values(by=self.pandas_sort_col)

            calendar_sort_df["core_sequence"] = range(1, len(calendar_sort_df) + 1)
            if growth_type == "p/p":
                calendar_sort_df['growth_sequence'] = calendar_sort_df['core_sequence'] + 1
            else:
                growth_granularity = self.date_sort_col.replace(DATE_SEQ_COL_SUFFIX, '') if self.date_sort_col else self.time_granularity
                if growth_granularity not in self.periods_in_year:
                    raise exit_with_status("Please ask user to reach out to admin to setup periods in year for dataset to run year over year growth analysis.")
                calendar_sort_df['growth_sequence'] = calendar_sort_df['core_sequence'] + self.periods_in_year.get(growth_granularity)
            growth_df = pd.merge(growth_df, calendar_sort_df, on=self.pandas_sort_col, how='left')

            # handle where there is no growth data available
            if growth_df[~pd.isnull(growth_df['metric'])]["growth_sequence"].min() not in list(growth_df[~pd.isnull(growth_df['metric'])]["core_sequence"]):
                self.env.growth_type = "none"
                return df, metrics
            join_cols = [col for col in ['dim_val', 'dim', 'metric'] if col in growth_df.columns]

            # self join to get previous period columns
            growth_df = pd.merge(growth_df, growth_df, left_on=['core_sequence', *join_cols], right_on=['growth_sequence', *join_cols], how='left', suffixes=('', '_prev'))

            # remove rows with missing metric, this can happen if data is missing for entire period for all dimension values
            growth_df = growth_df[~pd.isnull(growth_df['metric'])]

            if growth_df.empty:
                raise exit_with_status("No growth data available for the selected set of filters. Please ask user to change the filter criteria or ask a different question to proceed.")
            # filter the data to requested dates
            if self.start_seq and self.end_seq:
                growth_df = growth_df[(growth_df[self.pandas_sort_col] >= self.start_seq) & (growth_df[self.pandas_sort_col] <= self.end_seq)]
            elif self.start_date and self.end_date:
                growth_df = growth_df[(growth_df[self.pandas_sort_col] >= pd.to_datetime(self.start_date)) & (growth_df[self.pandas_sort_col] <= pd.to_datetime(self.end_date))]
            # calculate delta growth
            growth_df['value'] = growth_df['value'] - growth_df['value_prev']
            # delta metrics
            delta_df = copy.deepcopy(growth_df)
            delta_df['metric'] = delta_df['metric'].apply(lambda x: f"{x}{DELTA}")
            delta_df = delta_df.drop(columns=['growth_sequence', 'core_sequence', *[col for col in delta_df.columns if col.endswith('_prev')]])
            # calculate percentage growth for relevant metrics
            growth_df['value'] = np.where(growth_df['metric'].isin(percentage_growth),
                                          np.where(growth_df['value_prev'] != 0,
                                                   growth_df['value'] / np.abs(growth_df['value_prev']),
                                                   np.nan),  # Default value if condition is false
                                          growth_df['value'])  # Default value if outer condition is false
            growth_df = growth_df.drop(columns=['growth_sequence', 'core_sequence', *[col for col in growth_df.columns if col.endswith('_prev')]])
            growth_df['metric'] = growth_df['metric'].apply(lambda x: f"{x}{GROWTH}")
            # add original metrics back to the df
            # filter the data to only dates when growth is available
            max_date, min_date = growth_df[self.pandas_sort_col].max(), growth_df[self.pandas_sort_col].min()
            df = df[(df[self.pandas_sort_col] >= min_date) & (df[self.pandas_sort_col] <= max_date)]
            df['metric'] = df['metric'].apply(lambda x: x.replace(GROWTH, "") if x.endswith(GROWTH) else x)
            # remove nan values for warning
            available_growth = growth_df[~pd.isnull(growth_df['value'])]
            print(f"{df[self.pandas_sort_col].min()} -- {available_growth[self.pandas_sort_col].min()}")
            if df[self.pandas_sort_col].min() < available_growth[self.pandas_sort_col].min():
                self.warning_messages.append("Growth data is not available for the entire analysis period.")
            df = pd.concat([df, delta_df, growth_df], ignore_index=True).reset_index(drop=True)
            growth_metrics = [f"{m}{GROWTH}" for m in metrics]
            delta_metrics = [f"{m}{DELTA}" for m in metrics]
            metrics = metrics + growth_metrics + delta_metrics
        
        elif str(self.env.growth_type).lower() in VARIANCE_TYPES:

            variance_df = copy.deepcopy(df)

            # find metric in metric_props, add that to metrics
            variance_type = self.get_variance_type(self.env.growth_type)
            variance_metrics = [met for met in metrics if met.endswith(variance_type)]

            # pivot so that the 'metric' column has a column for each metric
            index_cols = ['date_column', self.date_alias, 'dim', 'dim_val'] if 'dim' in df.columns else ['date_column', self.date_alias]
            if 'date_order_col' in df.columns:
                index_cols.insert(0, 'date_order_col')  # index by date_order_col first since df is sorted by this col
            variance_df = variance_df.pivot(index=index_cols, columns='metric', values='value')

            additional_variance_metrics = []

            for variance_metric in variance_metrics:

                component_metric = variance_metric.replace(f"_{variance_type}", "")
                variance_difference_metric = f"{variance_metric}{VARIANCE_DELTA}"
                variance_growth_metric = f"{variance_metric}{VARIANCE_GROWTH}"

                additional_variance_metrics.append(variance_difference_metric)
                additional_variance_metrics.append(variance_growth_metric)

                # calculate the variance between the metric and the variance_metric
                variance_df[variance_difference_metric] = variance_df[component_metric] - variance_df[variance_metric]

                # calculate the variance growth
                component_metric_info = self.helper.get_metric_prop(component_metric, self.metric_props)
                is_delta_growth = component_metric_info.get('hide_percentage_change')

                if is_delta_growth:
                    variance_df[variance_growth_metric] = variance_df[variance_difference_metric]
                else:
                    variance_df[variance_growth_metric] = np.where(variance_df[variance_metric] != 0,
                                                                            variance_df[variance_difference_metric] / np.abs(variance_df[variance_metric]),
                                                                            np.nan)
                
            metrics = metrics + additional_variance_metrics

            # pivot back to long format
            variance_df = variance_df.reset_index()
            df = variance_df.melt(id_vars=index_cols, value_vars=[met for met in metrics if met in variance_df.columns], var_name='metric', value_name='value')

            metrics = metrics + additional_variance_metrics
                                                                
        return df, metrics

    def get_charts(self, df, metrics, dims):
        growth_type = str(self.env.growth_type).lower()
        charts = {}
        if growth_type in ["p/p", "y/y"]:
            absolute_metrics = [m for m in metrics if not m.endswith(GROWTH) and not m.endswith(DELTA)]
            delta_metrics = [m for m in metrics if m.endswith(DELTA)]
            growth_metrics = [m for m in metrics if m.endswith(GROWTH)]
            if not dims:
                charts = self._get_charts(df, metrics, dims)
                y_axis_index = {}
                axis_info = {}
                for metric_type, metric_list in zip(["absolute", "difference", "growth"], [absolute_metrics, delta_metrics, growth_metrics]):
                    type_chart = self._get_charts(df, metric_list, dims)
                    type_chart = type_chart[list(type_chart.keys())[0]]
                    type_yaxis = type_chart.get('yaxis')
                    type_y_axis_index = type_chart.get('yaxis_index')
                    type_chart_df = type_chart.get('df')
                    if len(metric_list) > 1:
                        y_axis_index = {**y_axis_index, **type_y_axis_index}
                    else:
                        y_axis_index[metric_list[0]] = 0
                    axis_info[metric_type] = {"yaxis": type_yaxis}
                charts[list(charts.keys())[0]]['axis_info'] = axis_info
                charts[list(charts.keys())[0]]['yaxis_index'] = y_axis_index
                charts[list(charts.keys())[0]]['tab_number'] = 1
                charts[list(charts.keys())[0]]['default_chart'] = self.abs_default_series
                charts[list(charts.keys())[0]]['xAxis'] = list(dict.fromkeys(type_chart_df[self.date_alias].to_list()))
            else:
                counter = 0
                for metric in absolute_metrics:
                    for dim in dims:
                        counter += 1
                        component_metrics = [metric, f"{metric}{GROWTH}", f"{metric}{DELTA}"]
                        # a new chart is created each loop
                        axis_info = {}
                        chart_df = df.copy()

                        # remove other metrics and other dims
                        chart_df = chart_df[(chart_df['metric'].isin(component_metrics)) & (chart_df['dim'] == dim)]
                        chart_df.drop(columns=['dim'], inplace=True)

                        # pivot chart_df so the values of the dim are columns, the jinja template expects it this way
                        index_cols = ['metric', 'date_column', self.date_alias]
                        if 'date_order_col' in df.columns:
                            index_cols.insert(0, 'date_order_col')  # index by date_order_col first since df is sorted by this col

                        chart_df = chart_df.pivot(index=index_cols, columns='dim_val', values='value').reset_index()
                        # non_null_columns = [col for col in chart_df.columns if col not in ['metric', 'date_column', 'quarter']]
                        # chart_df = chart_df.dropna(subset=non_null_columns, how='all', axis=0)

                        if "date_order_col" in chart_df.columns:
                            chart_df = chart_df.drop(columns=["date_order_col"])

                        met_prop = self.helper.get_metric_prop(metric, self.metric_props)
                        dim_prop = self.helper.get_dimension_prop(dim, self.dim_props)

                        metric_formats = {}
                        rename_metrics = {}
                        for comp_metric in component_metrics:
                            comp_prop = self.helper.get_metric_prop(comp_metric, self.metric_props)
                            metric_formats[comp_metric] = comp_prop.get('fmt')
                            rename_metrics[comp_metric] = comp_prop.get('label', comp_metric)
                            if comp_metric.endswith(GROWTH):
                                name = "growth"
                            elif comp_metric.endswith(DELTA):
                                name = "difference"
                            else:
                                name = "absolute"
                            axis_info[name] = {"yaxis": [{"title": comp_prop.get('label', comp_metric),
                                                          "fmt": comp_prop.get('fmt'),
                                                          "pretty_num": False if '%' in comp_prop.get('fmt') else True,
                                                          "opposite": False
                                                          }]}
                        chart_df['fmt'] = chart_df['metric'].apply(lambda x: metric_formats.get(x))
                        chart_df['metric'] = chart_df['metric'].apply(lambda x: rename_metrics.get(x))
                        # get chart name and store chart
                        met_label = met_prop.get("label", met_prop.get("name"))
                        dim_label = dim_prop.get("label", dim_prop.get("name"))

                        chart_name = f'{dim_label} · {met_label}' if len(dims) > 1 and len(metrics) > 1 else (dim_label if len(dims) > 1 else met_label)
                        chart_df['chart_name'] = chart_name
                        cols_to_keep = ['metric', 'date_column', self.date_alias, 'fmt', 'chart_name']
                        for col in chart_df.columns:
                            if col not in cols_to_keep:
                                cols_to_keep.append(col)
                                break

                        # add footer about share calculation
                        footnote = None
                        if met_prop.get("metric_type") == "share":
                            if self.footnotes.get(dim_label):
                                footnote = self.footnotes[dim_label]

                        charts[chart_name] = {
                            "df": chart_df,
                            "axis_info": axis_info,
                            "yaxis_index": {},
                            "dim_breakout": True,
                            "name": None,
                            "tab_number": counter,
                            "default_chart": self.abs_default_series,
                            "xAxis": list(dict.fromkeys(chart_df[self.date_alias].to_list())),
                            "footnote": footnote
                        }

        elif str(self.env.growth_type).lower() in VARIANCE_TYPES:
            variance_type = self.get_variance_type(self.env.growth_type)

            absolute_metrics = [m for m in metrics if not m.endswith(variance_type) and not m.endswith(VARIANCE_DELTA) and not m.endswith(VARIANCE_GROWTH)]
            variance_metrics = [m for m in metrics if m.endswith(variance_type)]
            delta_metrics = [m for m in metrics if m.endswith(VARIANCE_DELTA)]
            percent_delta_metrics = [m for m in metrics if m.endswith(VARIANCE_GROWTH)]

            if not dims:
                charts = self._get_charts(df, metrics, dims)

                y_axis_index = {}
                axis_info = {}
                for metric_type, metric_list in zip(["absolute", "difference", "growth"], [absolute_metrics, delta_metrics, percent_delta_metrics]):
                    type_chart = self._get_charts(df, metric_list, dims)
                    type_chart = type_chart[list(type_chart.keys())[0]]
                    type_yaxis = type_chart.get('yaxis')
                    type_y_axis_index = type_chart.get('yaxis_index')
                    type_chart_df = type_chart.get('df')
                    if len(metric_list) > 1:
                        y_axis_index = {**y_axis_index, **type_y_axis_index}
                    else:
                        y_axis_index[metric_list[0]] = 0
                    axis_info[metric_type] = {"yaxis": type_yaxis}

                charts[list(charts.keys())[0]]['axis_info'] = axis_info
                charts[list(charts.keys())[0]]['yaxis_index'] = y_axis_index
                charts[list(charts.keys())[0]]['tab_number'] = 1
                charts[list(charts.keys())[0]]['default_chart'] = self.abs_default_series
                charts[list(charts.keys())[0]]['xAxis'] = list(dict.fromkeys(type_chart_df[self.date_alias].to_list()))

            else:
                counter = 0
                for metric in variance_metrics:
                    for dim in dims:
                        counter += 1

                        component_metric = metric.replace(f"_{variance_type}", "")
                        component_metrics = [component_metric, f"{metric}{VARIANCE_DELTA}", f"{metric}{VARIANCE_GROWTH}"]
                        # a new chart is created each loop
                        axis_info = {}
                        chart_df = df.copy()

                        # remove other metrics and other dims
                        chart_df = chart_df[(chart_df['metric'].isin(component_metrics)) & (chart_df['dim'] == dim)]
                        chart_df.drop(columns=['dim'], inplace=True)

                        # pivot chart_df so the values of the dim are columns, the jinja template expects it this way
                        index_cols = ['metric', 'date_column', self.date_alias]
                        if 'date_order_col' in df.columns:
                            index_cols.insert(0, 'date_order_col')  # index by date_order_col first since df is sorted by this col

                        chart_df = chart_df.pivot(index=index_cols, columns='dim_val', values='value').reset_index()
                        # non_null_columns = [col for col in chart_df.columns if col not in ['metric', 'date_column', 'quarter']]
                        # chart_df = chart_df.dropna(subset=non_null_columns, how='all', axis=0)

                        if "date_order_col" in chart_df.columns:
                            chart_df = chart_df.drop(columns=["date_order_col"])

                        met_prop = self.helper.get_metric_prop(metric, self.metric_props)
                        dim_prop = self.helper.get_dimension_prop(dim, self.dim_props)

                        metric_formats = {}
                        rename_metrics = {}
                        for comp_metric in component_metrics:
                            comp_prop = self.helper.get_metric_prop(comp_metric, self.metric_props)
                            metric_formats[comp_metric] = comp_prop.get('fmt')
                            rename_metrics[comp_metric] = comp_prop.get('label', comp_metric)
                            if comp_metric.endswith(VARIANCE_GROWTH):
                                name = "growth"
                            elif comp_metric.endswith(VARIANCE_DELTA):
                                name = "difference"
                            else:
                                name = "absolute"
                            axis_info[name] = {"yaxis": [{"title": comp_prop.get('label', comp_metric),
                                                          "fmt": comp_prop.get('fmt'),
                                                          "pretty_num": False if '%' in comp_prop.get('fmt') else True,
                                                          "opposite": False
                                                          }]}
                        chart_df['fmt'] = chart_df['metric'].apply(lambda x: metric_formats.get(x))
                        chart_df['metric'] = chart_df['metric'].apply(lambda x: rename_metrics.get(x))
                        # get chart name and store chart
                        met_label = met_prop.get("label", met_prop.get("name"))
                        dim_label = dim_prop.get("label", dim_prop.get("name"))

                        chart_name = f'{dim_label} · {met_label}' if len(dims) > 1 and len(metrics) > 1 else (dim_label if len(dims) > 1 else met_label)
                        chart_df['chart_name'] = chart_name
                        cols_to_keep = ['metric', 'date_column', self.date_alias, 'fmt', 'chart_name']
                        for col in chart_df.columns:
                            if col not in cols_to_keep:
                                cols_to_keep.append(col)
                                break

                        # add footer about share calculation
                        footnote = None
                        if met_prop.get("metric_type") == "share":
                            if self.footnotes.get(dim_label):
                                footnote = self.footnotes[dim_label]

                        charts[chart_name] = {
                            "df": chart_df,
                            "axis_info": axis_info,
                            "yaxis_index": {},
                            "dim_breakout": True,
                            "name": None,
                            "tab_number": counter,
                            "default_chart": self.abs_default_series,
                            "xAxis": list(dict.fromkeys(chart_df[self.date_alias].to_list())),
                            "footnote": footnote
                        }

        else:
            charts = self._get_charts(df, metrics, dims)
            charts[list(charts.keys())[0]]['tab_number'] = 1

        return charts

    def create_notes(self, row, last_period):
        notes = []

        if str(self.growth_label).lower() == "yoy":
            growth = "vs year ago"
        else:
            growth = "vs previous period"

        abs_rank_col = 'Abs Rank'
        if abs_rank_col in row and 1 <= row[abs_rank_col] <= 5:
            try:
                rank = int(row[abs_rank_col])
            except:
                rank = row[abs_rank_col]
            notes.append(f"Top {rank} by {row['metric']} in {last_period}")

        growth_rank_col = 'Growth Rank'
        if growth_rank_col in row and row[growth_rank_col] == 1:
            notes.append(f"Highest growth {growth} in {last_period}")

        if growth_rank_col in row and row[growth_rank_col] == -1:
            notes.append(f"Bottom growth {growth} in {last_period}")

        difference_rank_col = 'Difference Rank'
        if difference_rank_col in row and row[difference_rank_col] == 1:
            notes.append(f"Highest diff {growth} in {last_period}")

        if difference_rank_col in row and row[difference_rank_col] == -1:
            notes.append(f"Bottom diff {growth} in {last_period}")

        abs_diff_rank_col = 'Abs Diff Rank'
        if abs_diff_rank_col in row and float(row[abs_diff_rank_col].split("__")[-1].strip()) == 1:
            label = row[abs_diff_rank_col].split("__")[0]
            notes.append(f"Top Abs {label}")

        if abs_diff_rank_col in row and float(row[abs_diff_rank_col].split("__")[-1].strip()) == -1:
            label = row[abs_diff_rank_col].split("__")[0]
            notes.append(f"Bottom Abs {label}")

        return ", ".join(notes)

    # updating facts per https://docs.google.com/spreadsheets/d/1YiG-00ROD28LJEliYU3DDrX9Yz7Tw-X6iCROVoqdAq4/edit?usp=sharing
    def get_facts_df(self, orig_df, top_growth=False, asc=False, top_n=1):
        non_metric_cols = ['metric', 'dim', 'dim_val', 'Rank', "Diff Rank"]
        df = orig_df.copy()
        growth_type = str(self.env.growth_type).lower()
        if growth_type not in ["p/p", "y/y"]:
            return self._get_facts_df(df, top_growth, asc, top_n)
        else:
            if top_growth:
                top_metrics = [m for m in df['metric'].unique() if not m.endswith(GROWTH) and not m.endswith(DELTA)]
                top_df, _ = self._get_facts_df(df[df['metric'].isin(top_metrics)], top_growth, asc, top_n, do_not_format=True, is_growth=True)
                top_metrics = list(top_df['metric'].unique())

                # growth calc between first and last periods
                delta_growth = []
                fmt_dict = {}
                growth_dict = {}
                for metric_name, metric_info in self.metric_props.items():
                    if metric_info.get('label') in top_metrics:
                        if metric_info.get('hide_percentage_change'):
                            delta_growth.append(metric_info.get('label'))
                        fmt_dict[metric_info.get('label')] = metric_info.get('fmt')
                        growth_dict[metric_info.get('label')] = metric_info.get('growth_fmt')

                delta_metrics = [m for m in top_df['metric'].unique() if m in delta_growth]

                # first column aprart from metric, dim and dim_val is first period col
                period_cols = [col for col in top_df.columns if col not in non_metric_cols]
                first_period_col = period_cols[0]
                growth_col = period_cols[-1]
                growth_col_rename = {growth_col: growth_col.replace("Difference", "Growth")}
                top_df[growth_col] = np.where(top_df['metric'].isin(delta_growth),
                                              top_df[growth_col],
                                              np.where(top_df[first_period_col] != 0,
                                                       top_df[growth_col] / top_df[first_period_col],
                                                       np.nan))
                # apply formatting
                top_df[growth_col] = top_df.apply(lambda row: self.helper.get_formatted_num(row[growth_col], growth_dict.get(row['metric']), pretty_num=True) if isinstance(row[growth_col], (int, float)) else row[growth_col], axis=1)
                for col in period_cols[:-1]:
                    top_df[col] = top_df.apply(lambda row: self.helper.get_formatted_num(row[col], fmt_dict.get(row['metric']), pretty_num=True) if isinstance(row[col], (int, float)) else row[col], axis=1)
                top_df = top_df.rename(columns=growth_col_rename)
                for col in ['Rank', "Diff Rank"]:
                    if col in top_df.columns:
                        top_df = top_df.drop(columns=[col])
                return top_df, list(top_df.columns)
            if df.empty:
                return pd.DataFrame(), []

            # tranform the data to new structure
            current_df, summary_cols = self._get_facts_df(df, is_growth=True)
            current_df.columns = current_df.columns.astype(str)
            summary_cols = [str(col) for col in summary_cols]
            period_cols = [col for col in current_df.columns if col not in non_metric_cols and "highest value" not in col.lower() and "lowest value" not in col.lower()]
            first_period_col = period_cols[0]
            growth_col = period_cols[-1]
            last_period_col = period_cols[-2] if len(period_cols) > 2 else first_period_col
            id_cols = ['metric', 'core_metric', 'metric_type']
            is_breakout = False
            if 'dim' in current_df.columns:
                id_cols.extend(['dim', 'dim_val'])
                is_breakout = True
            current_df["core_metric"] = current_df['metric'].apply(lambda x: x.split(self.growth_label)[0].strip())
            current_df["metric_type"] = current_df.apply(lambda row: row['metric'].replace(row['core_metric'], "").replace(self.growth_label, "").strip() or "Abs", axis=1)
            val_cols = [col for col in current_df.columns if col not in id_cols]
            melted_df = pd.melt(current_df, id_vars=id_cols, value_vars=val_cols)
            melted_df['metric_type'] = melted_df['metric_type'].apply(lambda x: x.replace("% Change", "Growth"))
            melted_df['new_cols'] = melted_df.apply(lambda row: f"{row['metric_type']} {row['variable']}", axis=1)
            diff_cols = [col for col in melted_df['new_cols'].unique() if col.lower().endswith("difference")]
            melted_df = melted_df.drop(columns=['metric', 'metric_type', 'variable'])
            pivot_df = melted_df.pivot(index=[col for col in id_cols if col in melted_df.columns], columns='new_cols', values='value')
            pivot_df = pivot_df.reset_index()
            pivot_df.columns.name = None
            if top_growth:
                drop_diff_cols = [col for col in melted_df['new_cols'].unique() if col.lower().endswith("difference") and not col.lower().startswith("growth")]
                pivot_df = pivot_df.drop(columns=drop_diff_cols)
            else:
                drop_diff_cols = [col for col in melted_df['new_cols'].unique() if col.lower().endswith("difference") and not col.lower().startswith("abs")]
                pivot_df = pivot_df.drop(columns=drop_diff_cols)
            pivot_df = pivot_df.rename(columns={"core_metric": "metric"})

            # keep only columns that required for the insights
            new_summary_cols = [col if col in id_cols else f"Abs {col}" for col in summary_cols] + diff_cols
            if is_breakout:
                pivot_df[f"Notes"] = pivot_df.apply(lambda row: self.create_notes(row, last_period_col), axis=1)
                pivot_df = pivot_df[pivot_df["Notes"] != ""]
                new_summary_cols.extend([f"Growth {last_period_col}", f"Difference {last_period_col}"])
                new_summary_cols = [col for col in new_summary_cols if not "highest value" in col.lower() and "lowest value" not in col.lower()]
                new_summary_cols.append(f"Notes")
            else:
                new_summary_cols.extend([f"{n} {t} Value" for n in ["Abs", "Growth", "Difference"] for t in ["Highest", "Lowest"]])
                new_summary_cols.extend([f"Growth {col}" for col in summary_cols if col not in id_cols])
                new_summary_cols.extend([f"Difference {col}" for col in summary_cols if col not in id_cols])
            summary_cols = [col for col in summary_cols if not col.lower().endswith("difference") and not col in ["Highest Value", "Lowest Value"]]

            # remove any columns that has all Nans
            pivot_df = pivot_df.dropna(axis=1, how='all')
            new_summary_cols = [col for col in new_summary_cols if col in pivot_df.columns]
            # remove duplicate summary cols, keep order
            new_summary_cols = list(dict.fromkeys(new_summary_cols))
            if top_growth:
                pivot_df = pivot_df[new_summary_cols]
            return pivot_df, new_summary_cols