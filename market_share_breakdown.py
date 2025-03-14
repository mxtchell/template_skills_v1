import pandas as pd
import numpy as np
import time
import re
import itertools

from dateutil.relativedelta import relativedelta

from ar_analytics.helpers.utils import SkillPlatform, TemplateParameterSetup, Connector, DimensionHierarchy, pull_data, \
    sparkline, SharedFn, \
    get_viz_header, old_get_date_label_str, NO_LIMIT_N, old_split_dim_and_metric_filters, old_get_filters_headline, \
    exit_with_status


class MarketShareBreakdown:
    def __init__(self, sql_exec=None, dim_hierarchy={}, constrained_values={}, compare_date_warning_msg=''):
        # database connection
        if sql_exec:
            self.con = sql_exec
        else:
            raise exit_with_status("Ask user to reach out to admin to setup a connection type.")

        if not dim_hierarchy:
            raise exit_with_status("Ask user to reach out to admin to setup a dimension hierarchy on dataset.")

        # these get set in run_from_env()
        self.sp = None
        self.use_max_sql_gen = True

        self.dim_hierarchy = DimensionHierarchy(dim_hierarchy)
        self.helper = SharedFn()
        self.allowed_metrics = constrained_values.get("metric", [])
        self.allowed_breakouts = constrained_values.get("breakout", [])
        self.notes = []
        self.dimensions_analyzed = []
        self.subject_row = None
        self.hit_row_limit = False
        self.compare_date_warning_msg = compare_date_warning_msg

    @classmethod
    def from_env(cls, env):

        if env is None:
            raise exit_with_status("env is required")

        cls.env = env

        return cls(
            sql_exec=env.msb_parameters["con"],
            dim_hierarchy=env.msb_parameters["dim_hierarchy"],
            constrained_values=env.msb_parameters["constrained_values"],
            compare_date_warning_msg=env.msb_parameters["compare_date_warning_msg"]
        )

    def get_share_and_component_metric(self, input_metric, metric_props):
        """
        Get the matching component metric and share metric based on the input component/share metric
        """

        # get metric properties of original metric
        metric = self.helper.get_metric_prop(input_metric, metric_props)

        # set metric and share metric
        share_metric = None
        if metric.get("metric_type") == "share":
            component_metric = metric.get("component_metric")
            if component_metric.lower() in [k.lower() for k in metric_props.keys()]:
                share_metric = metric
                metric = self.helper.get_metric_prop(component_metric, metric_props)
            else:
                raise exit_with_status("Share metric must have component_metric defined")
        else:
            # search for matching share metric based on component_metric
            for metric_key in metric_props:
                metric_dict = self.helper.get_metric_prop(metric_key, metric_props)
                if metric_dict.get("component_metric") and metric_dict.get(
                        "component_metric").lower() == input_metric.lower() and metric_dict.get(
                        "metric_type") == "share":
                    share_metric = metric_dict
                    break
            # default share metric
            if not share_metric:
                share_metric = {
                    "name": f"{metric['name']} Share",
                    "label": f"{metric['label']} Share",
                    "component_metric": metric['name'],
                    "sql": None,
                    "col": None,
                    "metric_type": "share",
                    "fmt": ",.2%",
                    "growth_fmt": "bps",
                    "hide_percentage_change": True,
                }

        return metric, share_metric

    def check_row_limit(self, df: pd.DataFrame):
        if not self.hit_row_limit:
            self.hit_row_limit = len(df) == self.con.limit

    def get_warning_messages(self):

        warning_messages = []

        if self.hit_row_limit:
            msg = f'The following analysis has been limited to {self.helper.get_formatted_num(self.con.limit, ",.0f")} rows which may impact the accuracy of the observations made.'
            warning_messages.append(msg)

        if self.compare_date_warning_msg:
            warning_messages.append(self.compare_date_warning_msg)

        return ' '.join(warning_messages) if warning_messages else None

    # replace special chars
    def replace_special_chars(self, value):
        if not value:
            return value
        return re.sub(r'[^a-zA-Z0-9]', '_', value)

    def get_default_table_jinja(self):
        return """
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>My Table</title>
                    <style>
                        table {
                            min-width: 100%;
                            width: auto;
                            border-collapse: separate;
                            border-spacing: 0;
                        }
                        thead {
                            position: sticky;
                            top: -5px;
                            z-index: 2;
                        }
                        th {
                            border-bottom: 1px solid #c0c0c0;
                            padding: 8px;
                            text-align: center;
                        }
                        td {
                            border-bottom: 1px solid #e0e0e0;
                            padding: 8px;
                        }
                        .spacer-col {
                            width: 2.5px;
                            border: 0px;
                            background-color: white;
                        }
                        tr.shown, tr.hidden {
                            display: table-row;
                        }
                        tr.hidden {
                            display: none;
                        }
                        .collapse-button {
                            font-size: 18px;
                            transition: transform 0.3s ease;
                            transform-origin: center;
                            border: none;
                            background: none;
                            cursor: pointer;
                            color: rgb(150, 150, 150);
                        }
                        .collapse-button[aria-expanded="true"] {
                            transform: rotate(90deg);
                        }
                    </style>
                </head>
                <body>
                    {% set share_metric_name = msb.share_metric['name'] %}
                    <div style="margin-top: -5px;">
                        <table>
                            <thead>
                                <!-- Set for metric drivers -->
                                {% if msb.include_drivers %}
                                    {% if 'subject_df' in df.columns %}
                                        {% set colspan = msb.subject_metric_drivers_colspan %}
                                    {% else %}
                                        {% set colspan = msb.decomposition_metric_drivers_colspan %}
                                    {% endif %}
                                {% endif %}
                                <tr>
                                    <!-- Main share metric values -->
                                    <th colspan="7" style="text-align: center;"><b>{{ metric_props[share_metric_name].get('label', share_metric_name) }}</b></th>
                                    <!-- Metric drivers -->
                                    {% if msb.include_drivers %}
                                        {% if 'subject_df' in df.columns %}
                                            {% for subject_section in msb.subject_metric_drivers %}
                                                <th class="spacer-col"/>
                                                <th colspan="{{ msb.subject_metric_drivers[subject_section]|length }}" style="text-align: center;"><b>{{ subject_section }}</b></th>
                                            {% endfor %}
                                        {% else %}
                                            {% for decomposition_section in msb.decomposition_metric_drivers %}
                                                <th class="spacer-col"/>
                                                <th colspan="{{ msb.decomposition_metric_drivers[decomposition_section]|length }}" style="text-align: center;"><b>{{ decomposition_section }}</b></th>
                                            {% endfor %}
                                        {% endif %}
                                    {% endif %}
                                </tr>
                                <tr>
                                    <th></th>
                                    <th>Share by {{ df['table_dims'].iloc[0] }}</th>
                                    {% if msb.date_labels['start_date'] == msb.date_labels['end_date'] %}
                                        <th>{{ msb.date_labels['start_date'] }}</th>
                                    {% else %}
                                        <th>{{ msb.date_labels['start_date'] }} to {{ msb.date_labels['end_date'] }}</th>
                                    {% endif %}

                                    {% if msb.date_labels['compare_start_date'] == msb.date_labels['compare_end_date'] %}
                                        <th>{{ msb.date_labels['compare_start_date'] }}</th>
                                    {% else %}
                                        <th>{{ msb.date_labels['compare_start_date'] }} to {{ msb.date_labels['compare_end_date'] }}</th>
                                    {% endif %}
                                    <th>Share Change {{ growth_type }}</th>
                                    <th>L12M Chg Y/Y</th>
                                    <th>Monthly Share Value</th>
                                    {% if msb.include_drivers %}
                                        {% if 'subject_df' in df.columns %}
                                            {% for section in msb.subject_metric_drivers %}
                                                <th class="spacer-col"/>
                                                {% for driver in msb.subject_metric_drivers[section] %}
                                                    <th>{{ msb.metric_drivers_labels[driver] }}</th>
                                                {% endfor %}
                                            {% endfor %}
                                        {% else %}
                                            {% for section in msb.decomposition_metric_drivers %}
                                                <th class="spacer-col"/>
                                                {% for driver in msb.decomposition_metric_drivers[section] %}
                                                    <th>{{ msb.metric_drivers_labels[driver] }}</th>
                                                {% endfor %}
                                            {% endfor %}
                                        {% endif %}
                                    {% endif %}
                                </tr>
                            </thead>
                            <tbody>
                                {% for index, row in df.iterrows() %}
                                    {% if row['parent_dim_member'] %}
                                        {% set id = "hidden-" ~ msb.replace_special_chars(row['parent_dim_member']) %}
                                        {% set class = "hidden" %}
                                    {% else %}
                                        {% set id = msb.replace_special_chars(row['dim_member']) %}
                                        {% set class = "" %}
                                    {% endif %}
                                    <tr id="{{ id }}" class="{{ class }}" {% if row['is_subject'] %} style="background-color: rgb(255, 240, 190); font-weight: bold;" {% endif %}>
                                        {% if row['is_collapsible'] %}
                                            <td>
                                                {% set button_id = "expand-" ~ row['dim_member'] %}
                                                <button type="button" id="{{ button_id }}" class="collapse-button" aria-expanded="false" onclick="toggle(this.id,'#hidden-{{ id }}');">
                                                    >
                                                </button>
                                            </td>
                                        {% else %}
                                            <td></td>
                                        {% endif %}
                                        <td style="text-align: left">
                                            <span style="display: flex; align-items: center;">
                                                {% if row['level'] != 0 %}
                                                    {{ (('&nbsp;' * 3) * row['level'] + '-&nbsp;') | safe }}
                                                {% endif %}
                                                {{ sfn.click_html(row['dim_member'], row['msg']) }}
                                            </span>
                                        </td>
                                        <td style="text-align: center">{{ row['share_curr'] }}</td>
                                        <td style="text-align: center">{{ row['share_comp'] }}</td>
                                        <td style="text-align: center">{{ row['share_change'] }}</td>
                                        <td style="text-align: center">{{ row['share_change_mat'] }}</td>
                                        <td style="text-align: center">{{ sfn.click_html('<img height="35px" src="data:image/png;base64,' + row['sparkline'] + '"/>', row['trend_msg']) }}</td>
                                        {% if msb.include_drivers %}
                                            {% if 'subject_df' in df.columns %}
                                                {% for section in msb.subject_metric_drivers %}
                                                    <td class="spacer-col"/>
                                                    {% for driver in msb.subject_metric_drivers[section] %}
                                                        <td style="text-align: center">{{ row[driver] }}</td>
                                                    {% endfor %}
                                                {% endfor %}
                                            {% else %}
                                                {% for section in msb.decomposition_metric_drivers %}
                                                    <td class="spacer-col"/>
                                                    {% for driver in msb.decomposition_metric_drivers[section] %}
                                                        <td style="text-align: center">{{ row[driver] }}</td>
                                                    {% endfor %}
                                                {% endfor %}
                                            {% endif %}
                                        {% endif %}
                                    </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </body>
                <script>
                    function toggle(btnID, eIDs) {
                        var theRows = document.querySelectorAll(eIDs);
                        var theButton = document.getElementById(btnID);
                        if (theButton.getAttribute("aria-expanded") == "false") {
                            for (var i = 0; i < theRows.length; i++) {
                                theRows[i].classList.add("shown");
                                theRows[i].classList.remove("hidden");
                            }
                            theButton.setAttribute("aria-expanded", "true");
                        } else {
                            for (var i = 0; i < theRows.length; i++) {
                                theRows[i].classList.add("hidden");
                                theRows[i].classList.remove("shown");
                            }
                            theButton.setAttribute("aria-expanded", "false");
                        }
                    }
                </script>
            </html>
        """

    def get_drilldown_message_suffix(self, market_filters, dim_props):
        # create comma separated string of dim label filter, dim label filter, etc for drilldown message
        dim_filter_str = ""
        dim_filter_list = []

        for f in market_filters:
            dim_label = dim_props.get(f['col'], {}).get('label', f['col'])
            dim_filter_list.append(f"{dim_label} {f['val']}")

        dim_filter_str = ", ".join(dim_filter_list)
        if dim_filter_str:
            dim_filter_str = f" with {dim_filter_str}"

        return dim_filter_str

    def single_quoted(self, l: list) -> str:
        if not l:
            return "()"
        l_out = []
        for s in l:
            # escape single quotes
            s = s.replace("'", "''")
            l_out.append(f"'{s}'")

        return "(" + ",".join(l_out) + ")"

    def get_analysis_message(self, metric, filters, date_labels):

        # get labels

        met_label = metric.get("label", metric.get("name"))

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

        analysis_message = f"Analysis ran for metric {met_label}"
        if filter_labels:
            analysis_message += f" filtered to {self.helper.and_comma_join(filter_labels)}"

        if date_label:
            analysis_message += f" for the period {date_label}"

        return analysis_message

    def process_subject_filter(self, query_filters):

        # get subject dim and member
        subject_filter = self.dim_hierarchy.get_lowest_level_ms_dim_filter(query_filters)
        subject_dim = subject_filter['col'] if subject_filter else None
        subject_member = subject_filter['val'] if subject_filter else None

        # If there are multiple potential subject dims, keep the lowest and drop the rest
        potential_subject_dims = list(
            set([f['col'] for f in query_filters if f['col'] in self.dim_hierarchy.owner_cols]))
        if len(potential_subject_dims) > 1:
            query_filters = [f for f in query_filters if f['col'] not in potential_subject_dims]

        if not subject_member:
            exit_with_status("Ask user to provide a subject filter to calculate market share.")

        # if there are more than 1 filter for the subject dim, drop the rest
        subject_filters = [f for f in query_filters if f['col'] == subject_dim]
        query_filters = [f for f in query_filters if f['col'] != subject_dim]
        if len(subject_filters) > 1:
            subject_dim_label = self.dim_props.get(subject_dim, {}).get('label', subject_dim)
            self.notes.append(
                f"Analysis focuses on the subject {subject_dim_label} '{subject_member}' only. You have been provided only limited facts. the user has a more complete table presented to them. Only respond using facts presented in the data above, let the user know more data might be presented on the table or chart on the screen.")

        query_filters.append(subject_filter)

        subject_member = subject_member.lower()

        return subject_dim, subject_member, query_filters

    def process_period_filters(self, period_filters):
        if not period_filters:
            raise exit_with_status("Ask user to provide a time range. Please do not make choice on user's behalf.")

        # period column
        self.period_col = period_filters[0]['col']

        if self.period_col.endswith("_sequence"):
            period_col_type = int
            periods_in_year_val = self.periods_in_year.get(self.period_col.replace("_sequence", ""))
        else:
            period_col_type = str
            periods_in_year_val = self.periods_in_year.get(self.default_date_granularity)

        # current period filter
        self.curr_period = period_filters[0]
        if self.curr_period['op'] == 'BETWEEN':
            self.curr_start_date, self.curr_end_date = self.curr_period['val'].replace("'", "").split(" AND ")
        else:
            self.curr_start_date = self.curr_end_date = self.curr_period['val'].replace("'", "")

        if (not self.curr_start_date or self.curr_start_date.lower() == 'nan') or (
                not self.curr_end_date or self.curr_end_date.lower() == 'nan'):
            exit_with_status(
                "The specified period is not available in the data. Ask user to try a different time range.")

        # comparison period filter
        self.comp_period = period_filters[1]
        if self.comp_period['op'] == 'BETWEEN':
            self.comp_start_date, self.comp_end_date = self.comp_period['val'].replace("'", "").split(" AND ")
        else:
            self.comp_start_date = self.comp_end_date = self.comp_period['val'].replace("'", "")

        if (not self.comp_start_date or self.comp_start_date.lower() == 'nan') or (
                not self.comp_end_date or self.comp_end_date.lower() == 'nan'):
            exit_with_status("Comparison period is not available in the data. Ask user to try a different time range.")

        # sparkline trend, always 2 years
        if len(period_filters) > 2:
            self.trend_period = period_filters[2]
            self.trend_start_date, self.trend_end_date = self.trend_period['val'].replace("'", "").split(" AND ")
            # MAT
            self.mat_start_date = int(self.trend_end_date) - (periods_in_year_val - 1)
            self.mat_end_date = self.trend_end_date
            self.mat_period = {'col': self.period_col, 'op': 'BETWEEN',
                               'val': f"'{self.mat_start_date} AND {self.mat_end_date}'"}

            # MAT compare
            self.mat_comp_start_date = int(self.trend_end_date) - periods_in_year_val - (periods_in_year_val - 1)
            self.mat_comp_end_date = int(self.trend_end_date) - periods_in_year_val
            self.mat_comp_period = {'col': self.period_col, 'op': 'BETWEEN',
                                    'val': f"'{self.mat_comp_start_date} AND {self.mat_comp_end_date}'"}

        else:
            if period_filters[0]['op'].lower() == 'between':
                curr_period = period_filters[0]['val'].lower().split(' and ')[1]
            else:
                curr_period = period_filters[0]['val']
            period_col = period_filters[0]['col']

            trend_timedelta = relativedelta(years=2)

            start_period = (pd.to_datetime(curr_period) - trend_timedelta).strftime('%Y-%m-%d')
            curr_period = pd.to_datetime(curr_period).strftime('%Y-%m-%d')
            if not curr_period.startswith("'"):
                curr_period = f"'{curr_period}'"
            self.trend_period = {"col": period_col, "op": "BETWEEN", "val": f"'{start_period}' AND {curr_period}"}

            self.trend_start_date, self.trend_end_date = self.trend_period['val'].replace("'", "").split(" AND ")

            # MAT
            relative_delta = self.helper.get_relative_delta_for_period(date_granularity=self.default_date_granularity,
                                                                       periods_in_year=periods_in_year_val)
            relative_delta_offset_minus_1 = self.helper.get_relative_delta_for_period(
                date_granularity=self.default_date_granularity,
                periods_in_year=periods_in_year_val,
                offset=-1)

            self.mat_start_date = self.helper.get_start_of_granularity(pd.to_datetime(self.trend_end_date)
                                                                       - relative_delta_offset_minus_1,
                                                                       self.default_date_granularity).strftime(
                '%Y-%m-%d')
            self.mat_end_date = pd.to_datetime(self.trend_end_date).strftime('%Y-%m-%d')
            self.mat_period = {'col': self.period_col, 'op': 'BETWEEN',
                               'val': f"'{self.mat_start_date} AND {self.mat_end_date}'"}

            # MAT compare
            self.mat_comp_start_date = self.helper.get_start_of_granularity(
                pd.to_datetime(self.trend_end_date) - relative_delta
                - relative_delta_offset_minus_1, self.default_date_granularity).strftime('%Y-%m-%d')
            self.mat_comp_end_date = (pd.to_datetime(self.trend_end_date) - relative_delta).strftime('%Y-%m-%d')
            self.mat_comp_period = {'col': self.period_col, 'op': 'BETWEEN',
                                    'val': f"'{self.mat_comp_start_date} AND {self.mat_comp_end_date}'"}

        # convert sequences to int
        if period_col_type == int:
            self.curr_start_date = int(self.curr_start_date)
            self.curr_end_date = int(self.curr_end_date)
            self.comp_start_date = int(self.comp_start_date)
            self.comp_end_date = int(self.comp_end_date)
            self.trend_start_date = int(self.trend_start_date)
            self.trend_end_date = int(self.trend_end_date)
            self.mat_start_date = int(self.mat_start_date)
            self.mat_end_date = int(self.mat_end_date)
            self.mat_comp_start_date = int(self.mat_comp_start_date)
            self.mat_comp_end_date = int(self.mat_comp_end_date)

    def process_breakouts(self, breakouts, query_filters):

        def get_share_type_from_dim_hierarchy(dim):

            share_type = 'contribution'
            if not dim.lower() in self.dim_hierarchy.owner_cols and self.dim_hierarchy.is_dimension_in_hierarchy(dim):
                share_type = 'share'
            return share_type

        def modify_breakouts(breakouts, level=1):
            i = 0
            while i < len(breakouts):
                breakout = breakouts[i]
                breakout['level'] = level
                if breakout['dim'] == self.subject_dim:
                    if 'drilldown' not in breakout:
                        # remove breakout if no drilldown and dim matches subject_dim
                        breakouts.pop(i)
                        continue
                    else:
                        # if a drilldown exists, replace the breakout with its drilldown
                        if 'tab_label' in breakout:
                            breakout['drilldown']['tab_label'] = breakout['tab_label']
                        breakout['drilldown']['level'] = level
                        breakouts[i] = breakout['drilldown']

                elif 'drilldown' in breakout:
                    # check if the lowest level drilldown's dim matches subject_dim, if so, remove the whole breakout
                    if check_lowest_drilldown_dim(breakout['drilldown']):
                        breakouts.pop(i)
                        continue
                    else:
                        if 'tab_label' in breakout:
                            # ensure the tab_label is passed down to drilldowns
                            breakout['drilldown']['tab_label'] = breakout['tab_label']
                        # otherwise, recursively check the drilldowns
                        modify_breakouts([breakout['drilldown']], level + 1)

                if 'type' not in breakouts[i]:
                    breakouts[i]['type'] = get_share_type_from_dim_hierarchy(breakouts[i]['dim'])

                i += 1

        def check_lowest_drilldown_dim(breakout):
            if 'drilldown' in breakout:
                return check_lowest_drilldown_dim(breakout['drilldown'])
            return breakout['dim'] == self.subject_dim

        modify_breakouts(breakouts)

        # remove any breakouts that have a single filter for the same dim
        breakouts = [b for b in breakouts if len([f for f in query_filters if f['col'] == b['dim']]) != 1]

        return breakouts

    def format_df(self, df, driver_metrics=[]):
        df = df.copy()

        # metric formatting
        df['metric_curr'] = df['metric_curr'].apply(lambda x: self.helper.get_formatted_num(x, self.metric.get("fmt")))
        df['metric_comp'] = df['metric_comp'].apply(lambda x: self.helper.get_formatted_num(x, self.metric.get("fmt")))
        df['diff'] = df['diff'].apply(lambda x: self.helper.get_formatted_num(x, self.metric.get("fmt")))
        df['diff_pct'] = df['diff_pct'].apply(lambda x: self.helper.get_formatted_num(x, self.metric.get("growth_fmt")))

        # share formatting
        df['share_curr'] = df['share_curr'].apply(
            lambda x: self.helper.get_formatted_num(x, self.share_metric.get("fmt")))
        df['share_comp'] = df['share_comp'].apply(
            lambda x: self.helper.get_formatted_num(x, self.share_metric.get("fmt")))
        df['share_change'] = df['share_change'].apply(
            lambda x: self.helper.get_formatted_num(x, self.share_metric.get("growth_fmt")))
        df['share_change_mat'] = df['share_change_mat'].apply(
            lambda x: self.helper.get_formatted_num(x, self.share_metric.get("growth_fmt")))

        # rank formatting
        df['rank_change'] = df['rank_change'].apply(lambda x: self.helper.get_formatted_num(x, ",.0f", signed=True))

        if self.include_drivers:
            for driver_metric in driver_metrics:
                driver_metric_attrs = self.helper.get_metric_prop(driver_metric['name'], self.metric_props)
                df[driver_metric['name'] + '_curr'] = df[driver_metric['name'] + '_curr'].apply(
                    lambda x: self.helper.get_formatted_num(x, driver_metric_attrs.get("fmt")))
                df[driver_metric['name'] + '_comp'] = df[driver_metric['name'] + '_comp'].apply(
                    lambda x: self.helper.get_formatted_num(x, driver_metric_attrs.get("fmt")))
                df[driver_metric['name'] + '_diff'] = df[driver_metric['name'] + '_diff'].apply(
                    lambda x: self.helper.get_formatted_num(x, driver_metric_attrs.get("fmt")))
                df[driver_metric['name'] + '_diff_pct'] = df[driver_metric['name'] + '_diff_pct'].apply(
                    lambda x: self.helper.get_formatted_num(x, driver_metric_attrs.get("growth_fmt")))

            if '__token__fair_share' in df.columns:
                df['__token__fair_share'] = df['__token__fair_share'].apply(
                    lambda x: self.helper.get_formatted_num(x, self.metric.get("fmt"), signed=True,
                                                            pretty_num=True) if isinstance(x, (int, float)) else x)

            for impact_metric in self.impact_metrics + self.share_impact_metrics:
                if f'{impact_metric["name"]}_impact' in df.columns:
                    df[f'{impact_metric["name"]}_impact'] = df[f'{impact_metric["name"]}_impact'].apply(
                        lambda x: self.helper.get_formatted_num(x, self.share_metric.get("growth_fmt"), signed=True))

        return df

    def get_subject_facts(self, query_metrics):
        # includes subject row facts containing subject_metric_drivers and decomposition_metric_drivers
        driver_metrics = [item for k, v in self.subject_metric_drivers.items() for item in v] + [item for k, v in
                                                                                                 self.decomposition_metric_drivers.items()
                                                                                                 for item in v]

        # format subject row
        self.subject_row = self.format_df(self.subject_row, query_metrics)
        fact_cols = self.core_fact_cols.copy()

        if self.include_drivers:
            fact_cols.extend(driver_metrics)

        subject_facts = self.subject_row[fact_cols].copy()

        # share impact is simply share change for share facts
        subject_fact_rename_dict = self.facts_rename_dict.copy()
        for impact_metric in self.share_impact_metrics:
            impact_metric_label = impact_metric.get('label', impact_metric)
            subject_fact_rename_dict[f'{impact_metric["name"]}_impact'] = f'{impact_metric_label} Change'

        subject_facts.rename(columns=subject_fact_rename_dict, inplace=True)

        return subject_facts

    def get_market_share_subject_facts(self, df, query_metrics=[]):

        subject_df = self.format_df(df, query_metrics)
        fact_cols = ["dim_member", "share_curr"]

        market_share_facts = subject_df[fact_cols].copy()

        # share impact is simply share change for share facts
        subject_fact_rename_dict = self.facts_rename_dict.copy()
        for impact_metric in self.share_impact_metrics:
            impact_metric_label = impact_metric.get('label', impact_metric)
            subject_fact_rename_dict[f'{impact_metric["name"]}_impact'] = f'{impact_metric_label} Change'

        market_share_facts.rename(columns=subject_fact_rename_dict, inplace=True)

        return market_share_facts

    def get_peers_facts(self, df, query_metrics=[]):
        df = df.copy()

        # includes facts containing subject_metric_drivers
        driver_metrics = [item for k, v in self.subject_metric_drivers.items() for item in v]

        # split peer facts into top 3 and bottom 3 by share_change
        top_facts = df[df['share_change'] > 0].nlargest(3, 'share_change')
        top_facts = self.format_df(top_facts, query_metrics)

        bottom_facts = df[df['share_change'] < 0].nsmallest(3, 'share_change')
        bottom_facts = self.format_df(bottom_facts, query_metrics)

        fact_cols = self.core_fact_cols.copy()

        if self.include_drivers:
            # for impact, include impact and _diff cols
            fact_cols.extend(driver_metrics)
            for impact_metric in self.impact_metrics:
                if f'{impact_metric["name"]}_impact' in df.columns:
                    fact_cols.append(f'{impact_metric["name"]}_diff')

        top_facts = top_facts[fact_cols].copy()
        bottom_facts = bottom_facts[fact_cols].copy()

        top_facts.rename(columns=self.facts_rename_dict, inplace=True)
        bottom_facts.rename(columns=self.facts_rename_dict, inplace=True)

        return top_facts, bottom_facts

    def get_breakout_facts(self, dfs):
        # concat all dfs except subject_df
        df_list = [df for label, df in dfs.items() if 'subject_df' not in df.columns]
        if len(df_list) == 0:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        all_breakouts = pd.concat(df_list, axis=0)

        # filter out subject row from breakouts
        all_breakouts = all_breakouts[all_breakouts['is_subject'] == False]

        # apply dim lables to dim and parent_dim
        all_breakouts['dim'] = all_breakouts['dim'].apply(lambda x: self.dim_props.get(x, {}).get('label', x))
        all_breakouts['parent_dim'] = all_breakouts['parent_dim'].apply(
            lambda x: self.dim_props.get(x, {}).get('label', x))

        # top and bottom breakouts by share impact
        top_breakouts = all_breakouts[all_breakouts[f'{self.share_metric["name"]}_impact'] > 0].nlargest(3,
                                                                                                         f'{self.share_metric["name"]}_impact')
        top_breakouts = self.format_df(top_breakouts)
        bottom_breakouts = all_breakouts[all_breakouts[f'{self.share_metric["name"]}_impact'] < 0].nsmallest(3,
                                                                                                             f'{self.share_metric["name"]}_impact')
        bottom_breakouts = self.format_df(bottom_breakouts)

        # bottom 3 dim_members by share_change
        bottom_growth_breakouts = all_breakouts.nsmallest(3, 'share_change')
        bottom_growth_breakouts = self.format_df(bottom_growth_breakouts)

        fact_cols = self.core_fact_cols.copy()
        fact_cols.extend(['share_type', 'parent_dim', 'parent_dim_member'])
        impact_cols = [f'{m["name"]}_impact' for m in self.impact_metrics]
        impact_share_cols = [f'{m["name"]}_impact' for m in self.share_impact_metrics]
        fact_cols.extend(impact_cols + impact_share_cols)

        top_breakouts = top_breakouts[fact_cols].copy()
        bottom_breakouts = bottom_breakouts[fact_cols].copy()
        bottom_growth_breakouts = bottom_growth_breakouts[fact_cols].copy()

        top_breakouts.rename(columns=self.facts_rename_dict, inplace=True)
        bottom_breakouts.rename(columns=self.facts_rename_dict, inplace=True)
        bottom_growth_breakouts.rename(columns=self.facts_rename_dict, inplace=True)

        return top_breakouts, bottom_breakouts, bottom_growth_breakouts

    def get_metric_driver_challenges_facts(self, dfs):
        # get bottom 3 dim_members by impact metric across all breakouts
        driver_challenges = {}

        # get top 5 dim_members by impact metric across all breakouts, excluding subject df and subject row
        dfs_concat = []
        for label, df in dfs.items():
            if 'subject_df' not in df.columns:
                df = df[df['is_subject'] == False]
                if df.empty:
                    continue
                if df['share_type'].iloc[0] == 'share':
                    dfs_concat.append(df.head(5))
                else:
                    # only include top 5 parent rows for contribution
                    df = df[df['parent_dim_member'].isna()]
                    dfs_concat.append(df.head(5))

        if len(dfs_concat) == 0:
            return pd.DataFrame()

        df = pd.concat(dfs_concat, axis=0)

        for impact_metric in self.impact_metrics:
            impact_metric = f'{impact_metric["name"]}_impact'
            bottom_challenges = df[df[impact_metric] < 0].nsmallest(3, impact_metric)
            bottom_challenges = self.format_df(bottom_challenges)
            bottom_challenges = bottom_challenges[['dim', 'dim_member', impact_metric]]
            bottom_challenges['Impact Metric'] = impact_metric
            bottom_challenges.rename(columns={impact_metric: 'Impact Value'}, inplace=True)
            driver_challenges[impact_metric] = bottom_challenges

        # concat into single df
        driver_challenges_df = pd.DataFrame()
        if len(driver_challenges) > 0:
            driver_challenges_df = pd.concat(driver_challenges.values(), axis=0)

        return driver_challenges_df

    def get_breakout_share(self, breakout_df, breakout_dim, curr_start_date, curr_end_date, comp_start_date,
                           comp_end_date):

        curr_df = breakout_df[
            (breakout_df[self.period_col] >= curr_start_date) & (breakout_df[self.period_col] <= curr_end_date)]
        curr_df = curr_df.groupby(breakout_dim)["metric", "market_size"].sum().reset_index()
        curr_df['share'] = curr_df["metric"].divide(curr_df["market_size"], fill_value=0)
        curr_df['rank'] = curr_df['metric'].rank(ascending=False, method='dense')

        comp_df = breakout_df[
            (breakout_df[self.period_col] >= comp_start_date) & (breakout_df[self.period_col] <= comp_end_date)]
        comp_df = comp_df.groupby(breakout_dim)["metric", "market_size"].sum().reset_index()
        comp_df['share'] = comp_df["metric"].divide(comp_df["market_size"], fill_value=0)
        comp_df['rank'] = comp_df['metric'].rank(ascending=False, method='dense')

        # merge mat_curr_df and mat_prev_df
        df = pd.merge(curr_df, comp_df, on=breakout_dim, how='outer', suffixes=('_curr', '_comp'))

        # remove rows where metric_curr and metric_comp are both 0/nan
        df = df[
            (df['metric_curr'] != 0 & df['metric_curr'].notna()) | (df['metric_comp'] != 0 & df['metric_comp'].notna())]

        # calculations
        df["metric_comp"] = df["metric_comp"].fillna(0)
        df["share_comp"] = df["share_comp"].fillna(0)
        df['diff'] = df['metric_curr'] - df['metric_comp']
        df['diff_pct'] = df['diff'].div(np.abs(df['metric_comp']), fill_value=0)
        df.replace([np.inf, -np.inf], 0, inplace=True)
        df["rank_change"] = np.where(pd.isnull(df['rank_comp']), 1 * df['rank_curr'],
                                     -1 * (df['rank_curr'] - df['rank_comp']))
        df["share_change"] = df['share_curr'] - df['share_comp']

        return df

    def get_dim_member_filters(self, df, dim, col='dim_member'):
        dim_members = df[col].unique().tolist()
        dim_member_filters = []

        if dim_members:
            dim_member_filters = [{
                'col': dim,
                'op': 'IN',
                'val': dim_members
            }]

        return dim_member_filters

    def get_drivers_df(self, table, breakout, driver_metrics, query_filters, dim_member_filters, share_type='share',
                       parent_breakout=None):

        driver_cols = [m['col'] for m in driver_metrics]
        market_rename_dict = {breakout: 'dim_member'}
        drivers_rename_dict = {breakout: 'dim_member'}
        for metric in driver_metrics:
            market_rename_dict[metric['col']] = metric['name'] + '_market_size'
            drivers_rename_dict[metric['col']] = metric['name']

        market_filters = [f for f in query_filters if f['col'] != self.subject_dim]

        # if breakout is subject_dim, then get market sizes for each metric without breakout
        if breakout == self.subject_dim:
            # set class variable for subject market size data for contribution
            self.df_market_curr = pull_data(metrics=driver_metrics, filters=market_filters + [self.curr_period])
            self.check_row_limit(self.df_market_curr)

            self.df_market_curr[driver_cols] = self.df_market_curr[driver_cols].astype(float)
            self.df_market_curr.rename(columns=market_rename_dict, inplace=True)

            self.df_market_comp = pull_data(metrics=driver_metrics, filters=market_filters + [self.comp_period])
            self.check_row_limit(self.df_market_comp)

            self.df_market_comp[driver_cols] = self.df_market_comp[driver_cols].astype(float)
            self.df_market_comp.rename(columns=market_rename_dict, inplace=True)

        else:
            # get current market size data
            df_market_curr = pull_data(metrics=driver_metrics, breakouts=[breakout],
                                       filters=market_filters + [self.curr_period])
            self.check_row_limit(df_market_curr)

            df_market_curr[driver_cols] = df_market_curr[driver_cols].astype(float)
            df_market_curr.rename(columns=market_rename_dict, inplace=True)

            # get comp market size data
            df_market_comp = pull_data(metrics=driver_metrics, breakouts=[breakout],
                                       filters=market_filters + [self.comp_period])
            self.check_row_limit(df_market_comp)

            df_market_comp[driver_cols] = df_market_comp[driver_cols].astype(float)
            df_market_comp.rename(columns=market_rename_dict, inplace=True)

        query_breakouts = [breakout]
        if parent_breakout:
            query_breakouts.append(parent_breakout)

        # get current driver data
        df_drivers_curr = pull_data(metrics=driver_metrics,
                                    filters=query_filters + dim_member_filters + [self.curr_period],
                                    breakouts=query_breakouts)
        self.check_row_limit(df_drivers_curr)

        df_drivers_curr[driver_cols] = df_drivers_curr[driver_cols].astype(float)
        df_drivers_curr.rename(columns=drivers_rename_dict, inplace=True)
        # merge in market size and calculate share
        if breakout == self.subject_dim or share_type == 'contribution':
            for metric in driver_metrics:
                df_drivers_curr[metric['name'] + '_market_size'] = \
                self.df_market_curr[metric['name'] + '_market_size'].values[0]
        else:
            df_drivers_curr = pd.merge(df_drivers_curr, df_market_curr, on='dim_member', how='inner')

        for metric in driver_metrics:
            df_drivers_curr[metric['name'] + '_share'] = df_drivers_curr[metric['name']].div(
                df_drivers_curr[metric['name'] + '_market_size'], fill_value=0)

        # get comp driver data
        df_drivers_comp = pull_data(metrics=driver_metrics,
                                    filters=query_filters + dim_member_filters + [self.comp_period],
                                    breakouts=query_breakouts)
        self.check_row_limit(df_drivers_comp)

        df_drivers_comp[driver_cols] = df_drivers_comp[driver_cols].astype(float)
        df_drivers_comp.rename(columns=drivers_rename_dict, inplace=True)
        # merge in market size and calculate share
        if breakout == self.subject_dim or share_type == 'contribution':
            for metric in driver_metrics:
                df_drivers_comp[metric['name'] + '_market_size'] = \
                self.df_market_comp[metric['name'] + '_market_size'].values[0]
        else:
            df_drivers_comp = pd.merge(df_drivers_comp, df_market_comp, on='dim_member', how='outer')
        for metric in driver_metrics:
            df_drivers_comp[metric['name'] + '_share'] = df_drivers_comp[metric['name']].div(
                df_drivers_comp[metric['name'] + '_market_size'], fill_value=0)

        join_dims = ['dim_member']
        if parent_breakout:
            join_dims.append(parent_breakout)

        df_drivers = pd.merge(df_drivers_curr, df_drivers_comp, on=join_dims, how='inner', suffixes=('_curr', '_comp'))
        df_drivers = pd.merge(df_drivers_curr, df_drivers_comp, on=join_dims, how='outer', suffixes=('_curr', '_comp'))
        df_drivers = df_drivers.fillna(0)
        for metric in driver_metrics:
            df_drivers[metric['name'] + '_share_change'] = df_drivers[metric['name'] + '_share_curr'] - df_drivers[
                metric['name'] + '_share_comp']
            df_drivers[metric['name'] + '_diff'] = df_drivers[metric['name'] + '_curr'] - df_drivers[
                metric['name'] + '_comp']
            df_drivers[metric['name'] + '_diff_pct'] = df_drivers[metric['name'] + '_diff'].div(
                np.abs(df_drivers[metric['name'] + '_comp']), fill_value=0)

        return df_drivers

    def process_share_df(self, df, dim):
        # get core period df and MAT period df
        core_df = self.get_breakout_share(df, dim, self.curr_start_date, self.curr_end_date, self.comp_start_date,
                                          self.comp_end_date)

        # check that the subject value is in the core_df
        if dim == self.subject_dim:
            if self.subject_member.lower() not in core_df[dim].str.lower().tolist():
                exit_with_status(
                    f"Subject {self.subject_dim} '{self.subject_member}' not found in data. Ask user to try a different subject.")

        mat_df = self.get_breakout_share(df, dim, self.mat_start_date, self.mat_end_date, self.mat_comp_start_date,
                                         self.mat_comp_end_date)

        # get trend df
        trend_df = df.copy()
        trend_df['share'] = trend_df["metric"].divide(trend_df["market_size"], fill_value=0)
        trend_df = trend_df.pivot(index=self.period_col, columns=dim, values='share').reset_index()

        # get sparkline for each dim member
        sparklines = {}
        for dim_member in trend_df.columns:
            if dim_member != self.period_col:
                sparklines[dim_member] = sparkline(trend_df[dim_member].to_list())

        # merge MAT share change to core df
        df = pd.merge(core_df, mat_df[[dim, 'share_change']], on=dim, how='left', suffixes=('', '_mat'))
        df.rename(columns={dim: 'dim_member'}, inplace=True)

        # add sparklines
        df['sparkline'] = df['dim_member'].apply(lambda x: sparklines[x] if x in sparklines else sparkline([np.nan]))

        # order by current metric
        df = df.sort_values(by='metric_curr', ascending=False, na_position='last')

        df['is_subject'] = False
        if dim == self.subject_dim:
            df['is_subject'] = df['dim_member'].apply(lambda x: x.lower() == self.subject_member)

        df['dim'] = dim

        return df

    def get_data_for_all_periods(self, df, dim):
        unique_periods = df[self.period_col].unique()
        unique_dim_members = df[dim].unique()
        # Create all possible combinations
        full_combinations = pd.DataFrame(list(itertools.product(unique_dim_members, unique_periods)),
                                         columns=[dim, self.period_col])
        # Merge with original data
        df = pd.merge(full_combinations, df, on=[dim, self.period_col], how='left')
        return df

    def transform_subject_df(self, subject_df, subject_dim, top_n):

        # get subject market size for each period
        self.subject_market_df = subject_df.groupby(self.period_col)['metric'].sum().reset_index()
        self.subject_market_df.rename(columns={'metric': 'market_size'}, inplace=True)

        # filter to current period to get top n members of subject dim
        curr_df = subject_df[
            (subject_df[self.period_col] >= self.curr_start_date) & (subject_df[self.period_col] <= self.curr_end_date)]
        if curr_df.empty:
            exit_with_status("No data available for the current period. Ask user to try a different time range.")
        if top_n:
            top_members = \
            curr_df.groupby(subject_dim)['metric'].sum().reset_index().sort_values(by='metric', ascending=False).head(
                top_n)[subject_dim].str.lower().tolist()
        else:
            top_members = curr_df[subject_dim].str.lower().tolist()
        top_members.append(self.subject_member)

        # filter to top members
        subject_df = subject_df[subject_df[subject_dim].str.lower().isin(top_members)]

        # make sure subject is available for all periods
        subject_df = self.get_data_for_all_periods(subject_df, dim=subject_dim)

        # merge in market size
        subject_df = pd.merge(subject_df, self.subject_market_df, on=self.period_col, how='inner')

        # process share df
        df = self.process_share_df(subject_df, subject_dim)

        return df

    def transform_breakout_df(self, breakout_df, breakout_dim, top_n=None):

        # filter to subject dim member
        breakouts_subject_df = breakout_df[breakout_df[self.subject_dim].str.lower() == self.subject_member]

        # filter to top n members of breakout
        if top_n:
            # filter to current period
            curr_df = breakouts_subject_df[(breakouts_subject_df[self.period_col] >= self.curr_start_date) & (
                        breakouts_subject_df[self.period_col] <= self.curr_end_date)]
            top_members = \
            curr_df.groupby(breakout_dim)['metric'].sum().reset_index().sort_values(by='metric', ascending=False).head(
                top_n)[breakout_dim].str.lower().tolist()
            breakouts_subject_df = breakouts_subject_df[
                breakouts_subject_df[breakout_dim].str.lower().isin(top_members)]

        breakouts_subject_df = breakouts_subject_df.groupby([self.period_col, breakout_dim])[
            'metric'].sum().reset_index()

        # df for market size by breakout dim_member
        breakout_market_df = breakout_df.groupby([self.period_col, breakout_dim])['metric'].sum().reset_index()
        breakout_market_df.rename(columns={'metric': 'market_size'}, inplace=True)

        # make sure breakout dim is available for all periods
        breakouts_subject_df = self.get_data_for_all_periods(breakouts_subject_df, dim=breakout_dim)

        # merge in market size
        merged_df = pd.merge(breakouts_subject_df, breakout_market_df, on=[self.period_col, breakout_dim], how='inner')

        # process share df
        df = self.process_share_df(merged_df, breakout_dim)

        return df

    def transform_contribution_df(self, df, dim, top_n=None):

        # filter to current period to get top n members of breakout
        if top_n:
            curr_df = df[(df[self.period_col] >= self.curr_start_date) & (df[self.period_col] <= self.curr_end_date)]
            top_members = \
            curr_df.groupby(dim)['metric'].sum().reset_index().sort_values(by='metric', ascending=False).head(top_n)[
                dim].str.lower().tolist()
            df = df[df[dim].str.lower().isin(top_members)]

        # make sure breakout dim is available for all periods
        df = self.get_data_for_all_periods(df, dim=dim)

        # merge in market size from subject_market_df
        df = pd.merge(df, self.subject_market_df, on=self.period_col, how='inner')

        # process share df
        df = self.process_share_df(df, dim)

        return df

    def safe_divide(self, numerator, denominator, fill_value=0):
        if denominator == 0 or denominator == 0.0:
            return fill_value
        else:
            return numerator / denominator

    def get_metrics_from_formula(self, formula: str):
        pattern = re.compile(r'\((\w+)\)')
        matches = pattern.findall(formula)
        metrics = list(set(matches))
        return metrics

    def formula_resolver(self, row, formula, term_mapping={}):
        # Uses same pattern as KPI Tree Impacts, for templatization of market share breakdown skill impacts
        # TODO make abs and round functions work

        if not term_mapping:
            term_mapping = {
                'current': '_curr',
                'prior': '_comp',
                'diff': '_diff',
                'prior_market': '_market_size_comp',
                'current_market': '_market_size_curr',
            }

        # replace the terms in the formula with the corresponding columns in the row
        pattern = re.compile(r'(\w+)\((\w+)\)')

        def replacer(match):
            prefix, term = match.groups()
            suffix = term_mapping.get(prefix, '')
            return f"row['{term}{suffix}']"

        transformed_formula = pattern.sub(replacer, formula)

        try:
            result = eval(transformed_formula)
        except Exception as e:
            print(f"Error evaluating the formula: {e}")
            return None

        return result

    def apply_fair_share(self, df):
        def calc_fair_share(self, row):
            if row['is_subject']:
                return '---'
            elif row['share_type'] == 'contribution':
                return np.nan
            else:
                return (row['share_curr'] - self.subject_row['share_curr'].values[0]) * row['market_size_curr']

        df['__token__fair_share'] = df.apply(lambda x: calc_fair_share(self, x), axis=1)

    def apply_share_impact_calcs(self, df, impact_share_metrics):
        def calc_share_impact(row, share_metric):
            component_metric = share_metric['component_metric']
            if row['is_subject']:
                # return share change for subject row
                return self.subject_row[f'{share_metric["name"]}_change'].values[0]
            elif row['share_type'] == 'contribution':
                # return share change for contribution rows
                return row[f'{share_metric["name"]}_change']
            else:
                # calculate share impact using component metric
                numerator = self.subject_row[f'{component_metric}_curr'].values[0] - row[f'{component_metric}_curr'] + \
                            row[f'{component_metric}_comp']
                denominator = self.subject_row[f'{component_metric}_market_size_curr'].values[0] - row[
                    f'{component_metric}_market_size_curr'] + row[f'{component_metric}_market_size_comp']
                return self.subject_row[f'{component_metric}_share_curr'].values[0] - (numerator / denominator)

        for share_metric in impact_share_metrics:
            df[f'{share_metric["name"]}_impact'] = df.apply(lambda x: calc_share_impact(x, share_metric), axis=1)

    def apply_calc_impacts(self, df, impact_metrics, impact_calcs):
        def calc_impact(row, calc_metric, impact_calcs):
            if calc_metric["name"] in impact_calcs:
                return self.formula_resolver(row, impact_calcs[calc_metric["name"]])
            else:
                raise exit_with_status(f'Calculation for {calc_metric["name"]} not found')

        for metric in impact_metrics:
            df[f'{metric["name"]}_impact'] = df.apply(lambda x: calc_impact(x, metric, impact_calcs), axis=1)

    def remove_html_brackets(self, text):
        if isinstance(text, str) and "<" in text and ">" in text:
            text = text.replace("<", "").replace(">", "")
        return text

    def get_display_tables(self, viz_func=None):
        tables = {}
        for tab_name, df in self.display_dfs.items():
            for col in ['dim_member', 'parent_dim_member', 'msg', 'trend_msg']:
                if col in df.columns:
                    # removing html brackets from dim_member and parent_dim_member here instead of in click_html
                    # because the click_html might have valid html, e.g. click on image run trend
                    df[col] = df[col].apply(lambda x: self.remove_html_brackets(x))

                cols_to_keep = ['parent_dim_member', 'dim_member', 'share_curr', 'share_comp', 'share_change',
                                'share_change_mat']
                col_rename_dict = {}
                col_rename_dict['dim_member'] = f"Share by {tab_name}"

                if self.date_labels.get('start_date') == self.date_labels.get('end_date'):
                    col_rename_dict['share_curr'] = str(self.date_labels.get('start_date'))
                else:
                    col_rename_dict[
                        'share_curr'] = f"{str(self.date_labels.get('start_date'))} to {str(self.date_labels.get('end_date'))}"

                if self.date_labels.get('compare_start_date') == self.date_labels.get('compare_end_date'):
                    col_rename_dict['share_comp'] = str(self.date_labels.get('compare_start_date'))
                else:
                    col_rename_dict[
                        'share_comp'] = f"{str(self.date_labels.get('compare_start_date'))} to {str(self.date_labels.get('compare_end_date'))}"

                col_rename_dict["share_change"] = f"Share Change {self.env.growth_type}"
                col_rename_dict["share_change_mat"] = f"L12M Chg Y/Y"

                df['dim_member'] = df['dim_member'].apply(lambda x: self.replace_special_chars(x))
                if 'parent_dim_member' in df.columns:
                    if df['parent_dim_member'].isnull().all():
                        cols_to_keep.remove('parent_dim_member')
                    else:
                        df['parent_dim_member'] = df['parent_dim_member'].apply(lambda x: self.replace_special_chars(x))
                if 'level' in df.columns:
                    df['dim_member'] = df.apply(
                        lambda row: f"{' ' * 3 * int(row['level'])}-{row['dim_member']}" if row['level'] > 0 else row[
                            'dim_member'], axis=1)
                    df["dim_member"] = df["dim_member"].apply(lambda x: x.replace("_", " "))

                if self.include_drivers:
                    if 'subject_df' in df.columns:
                        for section in self.subject_metric_drivers:
                            for driver in self.subject_metric_drivers[section]:
                                cols_to_keep.append(driver)
                                col_rename_dict[driver] = self.metric_drivers_labels[driver]
                    else:
                        for section in self.decomposition_metric_drivers:
                            for driver in self.decomposition_metric_drivers[section]:
                                cols_to_keep.append(driver)
                                col_rename_dict[driver] = self.metric_drivers_labels[driver]

            df = df[[col for col in cols_to_keep if col in df.columns]]
            df = df.rename(columns=col_rename_dict)

            tables[tab_name] = df

        return tables

    def run_from_env(self):

        if self.env is None:
            raise exit_with_status("self.env is required")

        self.sp = self.env.sp

        self.paramater_display_infomation = self.env.msb_parameters.get("ParameterDisplayDescription", {})

        market_filters = [f for f in self.env.msb_parameters.get("query_filters", []) if
                          f['col'] in self.env.msb_parameters.get("market_cols")]
        if len(market_filters) == 1:
            breakouts = self.env.msb_parameters["market_view"]
        else:
            breakouts = self.env.msb_parameters["global_view"]

        result_dfs = self.run(
            table=self.env.msb_parameters.get("table"),
            metric=self.env.msb_parameters.get("metric"),
            breakouts=breakouts,
            period_filters=self.env.msb_parameters.get("period_filters"),
            top_n=self.env.msb_parameters.get("limit_n", 10),
            query_filters=self.env.msb_parameters.get("query_filters"),
            metric_props=self.env.metric_props,
            dim_props=self.env.dim_props,
            include_drivers=self.env.msb_parameters.get("include_drivers"),
            view=self.env.msb_parameters.get("derived_sql_table"),
            date_labels=self.env.msb_parameters.get("date_labels"),
            growth_type=self.env.msb_parameters.get("growth_type"),
            periods_in_year=self.env.msb_parameters.get("periods_in_year"),
            impact_calcs=self.env.msb_parameters.get("impact_calcs"),
            subject_metric_config=self.env.msb_parameters.get("subject_metric_config"),
            decomposition_display_config=self.env.msb_parameters.get("decomposition_display_config"),
            default_date_granularity=self.env.msb_parameters.get("default_granularity")
        )

        # set the viz header
        self.env.viz__header = self.viz_header

        return result_dfs

    def set_metric_driver_html_sections(self, subject_metric_config, decomposition_display_config):
        """ Set the sections for the subject metric and decomposition tables for the html template """

        config_to_col_map = {
            "current": "_curr",
            "prior": "_comp",
            "diff": "_diff",
            "pct_change": "_diff_pct",
            "impact": "_impact"
        }

        label_map = {
            "_curr": "Current",
            "_comp": "Prior",
            "_diff": "Change",
            "_diff_pct": "Growth",
            "_impact": "Impact",
            f'__token__fair_share': 'Fair Share'
        }

        # get label dict for html column display
        self.metric_drivers_labels = {}

        self.subject_metric_drivers = {}
        for section in subject_metric_config:
            self.subject_metric_drivers[section] = []
            for metric, display_metrics in subject_metric_config[section].items():
                if metric == '__token__':
                    token_metric = f'__token__{display_metrics}'
                    self.subject_metric_drivers[section].append(token_metric)
                    self.metric_drivers_labels[token_metric] = label_map[token_metric]
                else:
                    for display_metric in display_metrics:
                        self.subject_metric_drivers[section].append(f'{metric}{config_to_col_map[display_metric]}')
                        display_metric_label = self.helper.get_metric_prop(metric, self.metric_props).get('label',
                                                                                                          metric)
                        self.metric_drivers_labels[
                            f'{metric}{config_to_col_map[display_metric]}'] = f'{display_metric_label} {label_map[config_to_col_map[display_metric]]}'

        self.decomposition_metric_drivers = {}
        for section in decomposition_display_config:
            self.decomposition_metric_drivers[section] = []
            for metric, display_metrics in decomposition_display_config[section].items():
                if metric == '__token__':
                    token_metric = f'__token__{display_metrics}'
                    self.decomposition_metric_drivers[section].append(token_metric)
                    self.metric_drivers_labels[token_metric] = label_map[token_metric]
                else:
                    for display_metric in display_metrics:
                        self.decomposition_metric_drivers[section].append(
                            f'{metric}{config_to_col_map[display_metric]}')
                        display_metric_label = self.helper.get_metric_prop(metric, self.metric_props).get('label',
                                                                                                          metric)
                        if metric in self.all_share_metrics and display_metric == "impact":
                            display_metric_suffix = "Contribution"
                        else:
                            display_metric_suffix = label_map[config_to_col_map[display_metric]]
                        self.metric_drivers_labels[
                            f'{metric}{config_to_col_map[display_metric]}'] = f'{display_metric_label} {display_metric_suffix}'

        # for colspan in html
        self.subject_metric_drivers_colspan = sum(
            [len(metrics) for metrics in self.subject_metric_drivers.values()]) + len(self.subject_metric_drivers) - 1
        self.decomposition_metric_drivers_colspan = sum(
            [len(metrics) for metrics in self.decomposition_metric_drivers.values()]) + len(
            self.decomposition_metric_drivers) - 1

    def get_driver_metrics(self, decomposition_display_config):
        """Get the metrics to calculate impacts for, decomposition metrics, and token metrics to display in the table"""

        decomp_metrics_names = []  # metrics to display in decomposition table
        impact_metric_names = []  # impact metrics used for impact calculations
        token_metrics = []  # custom metrics that are displayed in the table
        if decomposition_display_config:
            for section in decomposition_display_config:
                for metric, display_metrics in decomposition_display_config[section].items():
                    if 'impact' in display_metrics:
                        impact_metric_names.append(metric)
                    elif metric == '__token__':
                        token_metrics.append(display_metrics)
                    else:
                        decomp_metrics_names.append(metric)

        return decomp_metrics_names, impact_metric_names, token_metrics

    def get_metrics_for_query(self, subject_metric_config, impact_calcs, impact_metrics_names, decomp_metric_names,
                              metric_props):
        """ get all metrics needed for the query including impact calculation metrics, decomposition, and subject metrics"""

        # necessary metrics for impact calculations
        impact_calc_metrics = []
        if self.include_drivers:
            if impact_calcs:
                for impact_metric, impact_calc in impact_calcs.items():
                    if impact_metric in impact_metrics_names:
                        impact_calc_metrics.extend(self.get_metrics_from_formula(impact_calc))

        # metrics to display in subject table
        subject_driver_metrics = []
        if subject_metric_config:
            for section in subject_metric_config:
                for metric in subject_metric_config[section]:
                    if metric != '__token__':
                        subject_driver_metrics.append(metric)

        # all metrics needed for the query
        query_metrics = list(set(subject_driver_metrics + impact_calc_metrics + decomp_metric_names))
        query_metrics = [self.helper.get_metric_prop(m, metric_props) for m in query_metrics]
        return query_metrics

    def get_rename_dict(self, query_metrics, share_metric_label):
        facts_rename_dict = {
            'share_curr': f'Current {share_metric_label}',
            'share_comp': f'Compare {share_metric_label}',
            'share_change': f'{share_metric_label} Change',
            'share_change_mat': f'{share_metric_label} Change (MAT)',
        }

        for query_metric in query_metrics:
            query_metric_label = query_metric.get('label', query_metric['name'])
            facts_rename_dict[f'{query_metric["name"]}_curr'] = f'Current {query_metric_label}'
            facts_rename_dict[f'{query_metric["name"]}_comp'] = f'Compare {query_metric_label}'
            facts_rename_dict[f'{query_metric["name"]}_diff'] = f'{query_metric_label} Change'
            facts_rename_dict[f'{query_metric["name"]}_diff_pct'] = f'{query_metric_label} Change %'

        # impact metrics on share metric
        for impact_metric in self.impact_metrics:
            impact_metric_label = impact_metric.get('label', impact_metric)
            facts_rename_dict[
                f'{impact_metric["name"]}_impact'] = f'{impact_metric_label} Impact on {share_metric_label}'

        # share impacts on associated share metric
        for impact_metric in self.share_impact_metrics:
            impact_metric_label = impact_metric.get('label', impact_metric)
            facts_rename_dict[
                f'{impact_metric["name"]}_impact'] = f'{impact_metric_label} Impact on {impact_metric_label}'

        return facts_rename_dict

    def run(self, table, metric, period_filters, default_date_granularity, top_n=10, query_filters=[], breakouts=[],
            view="", metric_props={}, dim_props={}, include_drivers=False, date_labels={}, growth_type="",
            periods_in_year={}, impact_calcs={}, subject_metric_config={}, decomposition_display_config={}):

        self.metric, self.share_metric = self.get_share_and_component_metric(metric, metric_props)

        if self.share_metric not in metric_props.values():
            exit_with_status(
                f"Please inform user that the share metric {self.share_metric} not found in the dataset. Please choose a different metric.")

        self.dim_props = dim_props
        self.metric_props = metric_props
        self.periods_in_year = periods_in_year
        self.date_labels = date_labels
        self.default_date_granularity = default_date_granularity
        self.all_share_metrics = [met for met, props in metric_props.items() if
                                  props.get("metric_type") == "share" or props.get("is_share")]

        if isinstance(top_n, str):
            top_n = int(top_n)
        if top_n == 1:
            top_n = 10

        self.view = view

        # if not config found for subject_metric_config or decomposition_display_config, then don't include drivers
        if include_drivers and (subject_metric_config or decomposition_display_config):
            self.include_drivers = include_drivers
        else:
            self.include_drivers = False

        self.set_metric_driver_html_sections(subject_metric_config, decomposition_display_config)

        decomp_metrics_names, impact_metric_names, token_metrics = self.get_driver_metrics(decomposition_display_config)

        # get all metrics needed for the query
        query_metrics = self.get_metrics_for_query(subject_metric_config, impact_calcs, impact_metric_names,
                                                   decomp_metrics_names, metric_props)
        if set([m['name'] for m in query_metrics]) - set(list(self.metric_props.keys())):
            invalid_metrics = set([m['name'] for m in query_metrics]) - set(list(self.metric_props.keys()))
            exit_with_status(
                f"Invalid metrics {invalid_metrics} found in configuration. Ask user to reach out to admin to configure correct metrics from available metrics in dataset.")

        print(f"Query Metrics: {query_metrics}")

        # metric objects for impact and share impact metrics
        self.impact_metrics = [self.helper.get_metric_prop(m, metric_props) for m in impact_metric_names]
        self.share_impact_metrics = [m for m in self.impact_metrics if m.get('metric_type') == 'share']
        self.impact_metrics = [m for m in self.impact_metrics if m.get('metric_type') != 'share']

        share_metric_label = self.share_metric.get('label', self.share_metric['name'])

        self.subject_dim, self.subject_member, query_filters = self.process_subject_filter(query_filters)

        self.core_fact_cols = [
            'dim',
            'dim_member',
            'is_subject',
            'share_curr',
            'share_comp',
            'share_change',
            'share_change_mat',
        ]

        self.facts_rename_dict = self.get_rename_dict(query_metrics, share_metric_label)

        print("period_filters", period_filters)
        self.process_period_filters(period_filters)

        breakouts = self.process_breakouts(breakouts, query_filters)

        # if any invalid breakouts, exit with status
        if set([b.get('dim') for b in breakouts]) - set(dim_props.keys()):
            invalid_breakouts = set([b.get('dim') for b in breakouts]) - set(dim_props.keys())
            exit_with_status(
                f"Invalid breakouts {invalid_breakouts} found in configuration. Ask user to reach out to admin to configure correct dimension from available dimension in dataset.")

        print(f"\nBreakouts:\n{breakouts}")

        market_filters = [f for f in query_filters if f['col'] != self.subject_dim]

        dim_filter_str = self.get_drilldown_message_suffix(market_filters, dim_props)

        self.subject_dim_label = dim_props.get(self.subject_dim, {}).get('label', self.subject_dim)
        # self.notes.append(f"Analysis limited to the top {top_n} {self.subject_dim_label}s")

        dfs = {}
        subject_df = None
        start_time = time.time()

        # get subject df
        self.dimensions_analyzed.append(self.subject_dim)
        subject_df = pull_data(metrics=[self.metric], filters=market_filters + [self.trend_period],
                               breakouts=[self.subject_dim, self.period_col],
                               order_cols=[{"col": self.period_col, "direction": "ASC"}])
        self.check_row_limit(subject_df)

        if subject_df.empty:
            exit_with_status(
                f"No data found for {self.subject_dim_label} {self.subject_member}{dim_filter_str}. Ask user to try a different set of filters.")

        subject_df.rename(columns={self.metric['col']: 'metric'}, inplace=True)
        subject_df['metric'] = subject_df['metric'].astype(float)
        subject_df = self.transform_subject_df(subject_df, self.subject_dim, top_n)
        subject_df['level'] = 0
        subject_df['parent_dim'] = None
        subject_df['parent_dim_member'] = None
        subject_df['msg'] = subject_df.apply(lambda
                                                 x: f"Run the same analysis on {self.subject_dim_label} {x['dim_member']}{dim_filter_str} for same time period." if not
        x['is_subject'] else "", axis=1)
        subject_df['trend_msg'] = subject_df.apply(
            lambda x: f"Run {share_metric_label} trend for {self.subject_dim_label} {x['dim_member']}{dim_filter_str}",
            axis=1)
        subject_df['is_collapsible'] = False

        # get drivers metrics
        if self.include_drivers:
            dim_member_filters = self.get_dim_member_filters(subject_df, self.subject_dim)
            drivers_df = self.get_drivers_df(table, self.subject_dim, query_metrics, market_filters, dim_member_filters)
            subject_df = pd.merge(subject_df, drivers_df, on='dim_member', how='left')

        # save subject row
        self.subject_row = subject_df[subject_df['is_subject']].copy()

        if self.include_drivers:
            if 'fair_share' in token_metrics:
                self.apply_fair_share(self.subject_row)
            self.apply_share_impact_calcs(self.subject_row, self.share_impact_metrics)
            self.apply_calc_impacts(self.subject_row, self.impact_metrics, impact_calcs)

        # mark df as subject_df
        subject_df['subject_df'] = True

        dfs[self.subject_dim_label] = subject_df

        for breakout in breakouts:
            breakout_dim = breakout['dim']
            self.dimensions_analyzed.append(breakout_dim)
            breakout_dim_label = dim_props.get(breakout_dim, {}).get('label', breakout_dim)
            breakout_label = breakout.get("tab_label", breakout_dim_label)
            share_type = breakout.get("type")  # share or contribution

            if share_type == 'share':
                breakout_df = pull_data(metrics=[self.metric], filters=market_filters + [self.trend_period],
                                        breakouts=[breakout_dim, self.subject_dim, self.period_col],
                                        order_cols=[{"col": self.period_col, "direction": 'ASC'}])
                self.check_row_limit(breakout_df)

                breakout_df.rename(columns={self.metric['col']: 'metric'}, inplace=True)
                breakout_df['metric'] = breakout_df['metric'].astype(float)

                df = self.transform_breakout_df(breakout_df, breakout_dim, top_n=top_n)

                dim_member_filters = self.get_dim_member_filters(df, breakout_dim)

                if self.include_drivers:
                    drivers_df = self.get_drivers_df(table, breakout_dim, query_metrics, query_filters,
                                                     dim_member_filters)
                    df = pd.merge(df, drivers_df, on='dim_member', how='left')

                df['level'] = breakout['level']
                df['parent_dim'] = None
                df['parent_dim_member'] = None

                # add drilldown message
                df['msg'] = df.apply(lambda
                                         x: f"Run the same analysis on {x['dim']} {x['dim_member']} for {self.subject_dim_label} {self.subject_member}{dim_filter_str} for same time period.",
                                     axis=1)
                df['trend_msg'] = df.apply(lambda
                                               x: f"Run {share_metric_label} trend for {x['dim']} {x['dim_member']} for {self.subject_dim_label} {self.subject_member}{dim_filter_str}",
                                           axis=1)

                # make row non collapsable
                df['is_collapsible'] = False

            elif share_type == 'contribution':
                drilldown = breakout.get("drilldown")

                # pull data using query_filters
                df = pull_data(metrics=[self.metric], filters=[query_filters, self.trend_period],
                               breakouts=[breakout_dim, self.period_col],
                               order_cols=[{"col": self.period_col, "direction": 'ASC'}])
                self.check_row_limit(df)

                df.rename(columns={self.metric['col']: 'metric'}, inplace=True)
                df['metric'] = df['metric'].astype(float)

                # transform contribution df
                df = self.transform_contribution_df(df, breakout_dim, top_n=top_n)

                dim_member_filters = self.get_dim_member_filters(df, breakout_dim)

                if self.include_drivers:
                    drivers_df = self.get_drivers_df(table, breakout_dim, query_metrics, query_filters,
                                                     dim_member_filters, share_type='contribution')
                    df = pd.merge(df, drivers_df, on='dim_member', how='left')

                df['level'] = breakout['level']
                df['parent_dim'] = None
                df['parent_dim_member'] = None

                # add drilldown message
                df['msg'] = df.apply(lambda
                                         x: f"Run the same analysis for {breakout_dim_label} {x['dim_member']}{dim_filter_str} for same time period.",
                                     axis=1)
                df['trend_msg'] = df.apply(lambda
                                               x: f"Run {share_metric_label} trend for {breakout_dim_label} {x['dim_member']}{dim_filter_str}",
                                           axis=1)

                if drilldown:

                    # make parent row collapsable
                    df['is_collapsible'] = True

                    drilldown_dim = drilldown['dim']

                    # pull data for drilldown dim using query_filters + top dim_members of parent
                    self.dimensions_analyzed.append(drilldown_dim)
                    drilldown_df = pull_data(metrics=[self.metric],
                                             filters=query_filters + dim_member_filters + [self.trend_period],
                                             breakouts=[breakout_dim, drilldown_dim, self.period_col],
                                             order_cols=[{"col": self.period_col, "direction": "ASC"}])
                    self.check_row_limit(drilldown_df)

                    drilldown_df.rename(columns={self.metric['col']: 'metric'}, inplace=True)
                    drilldown_df['metric'] = drilldown_df['metric'].astype(float)

                    # get driver metrics for drilldown
                    dim_member_filters = self.get_dim_member_filters(drilldown_df, drilldown_dim, col=drilldown_dim)
                    if self.include_drivers:
                        drivers_df = self.get_drivers_df(table, drilldown_dim, query_metrics, query_filters,
                                                         dim_member_filters, share_type='contribution',
                                                         parent_breakout=breakout_dim)

                    # get drilldowns for each breakout dim member
                    breakout_dim_members = df['dim_member'].tolist()
                    for dim_member in breakout_dim_members:
                        if self.include_drivers:
                            drivers_df_filtered = drivers_df[drivers_df[breakout_dim] == dim_member].copy()
                            drivers_df_filtered.drop(columns=[breakout_dim], inplace=True)

                        drilldown_df_filtered = drilldown_df[drilldown_df[breakout_dim] == dim_member].copy()
                        drilldown_df_filtered.drop(columns=[breakout_dim], inplace=True)

                        # transform contribution df
                        drilldown_transformed_df = self.transform_contribution_df(drilldown_df_filtered, drilldown_dim,
                                                                                  top_n)
                        if self.include_drivers:
                            drilldown_transformed_df = pd.merge(drilldown_transformed_df, drivers_df_filtered,
                                                                on='dim_member', how='left')
                        drilldown_transformed_df['level'] = drilldown['level']
                        drilldown_transformed_df['parent_dim'] = breakout_dim
                        drilldown_transformed_df['parent_dim_member'] = dim_member

                        # add drilldown message
                        drilldown_dim_label = dim_props.get(drilldown_dim, {}).get('label', drilldown_dim)
                        drilldown_transformed_df['msg'] = drilldown_transformed_df.apply(lambda
                                                                                             x: f"Run the same analysis for {self.subject_dim_label} {self.subject_member}, {drilldown_dim_label} {x['dim_member']} in {breakout_dim_label} {dim_member}{dim_filter_str} for same time period.",
                                                                                         axis=1)
                        drilldown_transformed_df['trend_msg'] = drilldown_transformed_df.apply(lambda
                                                                                                   x: f"Run {share_metric_label} trend for {drilldown_dim_label} {x['dim_member']} in {breakout_dim_label} {dim_member}{dim_filter_str} for same time period.",
                                                                                               axis=1)

                        # make row non collapsable
                        drilldown_transformed_df['is_collapsible'] = False

                        # concat drilldown_transformed_df to df, by placing drilldown_transformed_df rows after the corresponding row of the parent dim_member
                        df = df.reset_index(drop=True)
                        idx = df.index[(df['dim_member'] == dim_member) & (df['dim'] == breakout_dim)].tolist()[0]
                        df = pd.concat([df.iloc[:idx + 1], drilldown_transformed_df, df.iloc[idx + 1:]]).reset_index(
                            drop=True)

            df['share_type'] = breakout['type']

            if self.include_drivers:
                if 'fair_share' in token_metrics:
                    self.apply_fair_share(df)
                self.apply_share_impact_calcs(df, self.share_impact_metrics)
                self.apply_calc_impacts(df, self.impact_metrics, impact_calcs)

            # concat subject row to the top for non-subject breakouts
            if isinstance(self.subject_row, pd.DataFrame):
                df = pd.concat([self.subject_row, df])

            if not df.empty:
                dfs[breakout_label] = df

        end_time = time.time()
        exec_time = end_time - start_time
        print(f"\nTotal Execution Time: {exec_time:.2f}s")

        # subject facts
        self.subject_facts = self.get_subject_facts(query_metrics)

        # peers facts
        self.top_peers_facts, self.bottom_peers_facts = self.get_peers_facts(subject_df, query_metrics)

        # market share subject facts
        self.market_share_subject_facts = self.get_market_share_subject_facts(subject_df, query_metrics)

        self.suggestions = []

        # breakout and driver facts rely on impacts
        self.top_breakouts_facts = pd.DataFrame()
        self.bottom_breakouts_facts = pd.DataFrame()
        self.metric_driver_challenges_facts = pd.DataFrame()
        if self.include_drivers:
            # breakout facts
            self.top_breakouts_facts, self.bottom_breakouts_facts, self.bottom_growth_breakouts = self.get_breakout_facts(
                dfs)
            # metric driver challenges facts
            self.metric_driver_challenges_facts = self.get_metric_driver_challenges_facts(dfs)

            # get suggestions based on bottom breakouts facts, use bottom growth if no negative impacts exist
            if self.bottom_breakouts_facts.empty:
                for row in self.bottom_growth_breakouts.itertuples():
                    if row.share_type == 'contribution':
                        if row.parent_dim and row.parent_dim_member:
                            self.suggestions.append({
                                                        "label": f"Analyze performance of {row.dim} {row.dim_member} in {row.parent_dim} {row.parent_dim_member}",
                                                        "question": f"Run the same analysis for {row.dim} {row.dim_member} in {row.parent_dim} {row.parent_dim_member}{dim_filter_str} same time period."})
                        else:
                            self.suggestions.append({"label": f"Analyze performance of {row.dim} {row.dim_member}",
                                                     "question": f"Run the same analysis for {row.dim} {row.dim_member}{dim_filter_str} same time period."})
                    else:
                        self.suggestions.append({"label": f"Analyze performance of {row.dim} {row.dim_member}",
                                                 "question": f"Run the same analysis on {row.dim} {row.dim_member} for {self.subject_dim_label} {self.subject_member}{dim_filter_str} same time period."})
            else:
                for row in self.bottom_breakouts_facts.itertuples():
                    if row.share_type == 'contribution':
                        if row.parent_dim and row.parent_dim_member:
                            self.suggestions.append({
                                                        "label": f"Analyze performance of {row.dim} {row.dim_member} in {row.parent_dim} {row.parent_dim_member}",
                                                        "question": f"Run the same analysis for {row.dim} {row.dim_member} in {row.parent_dim} {row.parent_dim_member}{dim_filter_str} same time period."})
                        else:
                            self.suggestions.append({"label": f"Analyze performance of {row.dim} {row.dim_member}",
                                                     "question": f"Run the same analysis for {row.dim} {row.dim_member}{dim_filter_str} same time period."})
                    else:
                        self.suggestions.append({"label": f"Analyze performance of {row.dim} {row.dim_member}",
                                                 "question": f"Run the same analysis on {row.dim} {row.dim_member} for {self.subject_dim_label} {self.subject_member}{dim_filter_str} same time period."})

        # format dfs
        for key, df in dfs.items():
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].astype(object)
            df[numeric_cols] = df[numeric_cols].fillna("Null")
            dfs[key] = self.format_df(df, query_metrics)

        # remove columns not applicable to contribution tables
        for key, df in dfs.items():
            if 'share_type' in df.columns and len(df) > 1:
                # index 1 to avoid subject row
                if df['share_type'].values[1] == 'contribution':
                    if 'fair_share' in df.columns:
                        df.drop(columns=['fair_share'], inplace=True)

        # generate table dims for column headers
        for key, df in dfs.items():
            table_dims = df[df['is_subject'] == False]['dim'].unique().tolist()
            table_dims = [dim_props.get(d, {}).get('label', d) for d in table_dims]
            table_dims = ' | '.join(table_dims)
            dfs[key]['table_dims'] = table_dims

        self.subject_member_label = self.subject_row['dim_member'].values[0]

        # market are all the filter vals that arent the subject dim
        market = [f for f in query_filters if f['col'] != self.subject_dim]
        market_str = old_get_filters_headline(query_filters=market, headline_seperator=', ', metric_props=metric_props,
                                              dim_props=dim_props) or "Total"
        title = f"Share of {self.subject_member_label} within {market_str}"
        if date_labels['start_date'] == date_labels['end_date']:
            date_str = date_labels['start_date']
        else:
            date_str = f"{date_labels['start_date']} to {date_labels['end_date']}"
        subtitle = f"{share_metric_label} Analysis • {date_str} {growth_type}"
        self.warning_message = self.get_warning_messages()
        self.title = title
        self.subtitle = subtitle
        self.viz_header = get_viz_header(title=title, subtitle=subtitle, warning_messages=self.get_warning_messages())

        if self.compare_date_warning_msg:
            self.notes.append(self.compare_date_warning_msg)

        self.notes.append(self.get_analysis_message(self.share_metric, query_filters, date_labels))
        # add notes about dimensions analyzed
        dimensions_analyzed_str = self.helper.and_comma_join(
            [dim_props.get(d, {}).get('label', d) for d in self.dimensions_analyzed if d])
        self.notes.append(f"Analysis only includes following dimensions {dimensions_analyzed_str}.")

        messages = self.notes or ["Analysis was completed as requested."]
        self.df_notes = pd.DataFrame({"Note to the assistant:": messages})

        self.display_dfs = dfs
        self.default_table_template = self.get_default_table_jinja()

        return dfs


class MSBTemplateParameterSetup(TemplateParameterSetup):
    def __init__(self, sp=None, env=None, default_sql_row_limit=200000):
        if sp is None:
            sp = SkillPlatform()

        super().__init__(sp=sp, default_sql_row_limit=default_sql_row_limit)
        # set the skill platform on env
        env.sp = sp

        self.map_env_values(env=env)

    """
    Creates parameters necessary to run the market share breakdown skill from the copilot skill parameter values and dataset.
    These are set on the env under msb_parameters, ie env.msb_parameters.

    Adds UI bubbles for certain parameters.
    """

    def map_env_values(self, env=None):

        """
        This function currently hardcodes the following names of the copilot skill variables in the templates, specifically
        TODO: Create way to reference these dynamically from the copilot skill.
        """

        if env is None:
            raise exit_with_status("env namespace is required.")

        msb_parameters = {}
        pills = {}

        # output configuration
        msb_parameters["market_view"] = env.market_view
        msb_parameters["global_view"] = env.global_view
        msb_parameters["subject_metric_config"] = env.subject_metric_config
        msb_parameters["decomposition_display_config"] = env.decomposition_display_config

        ## Setup DB

        database_id = self.dataset_metadata.get("database_id")
        msb_parameters["table"] = self.dataset_metadata.get("sql_table")
        msb_parameters["derived_sql_table"] = self.dataset_metadata.get("derived_table_sql") or ""
        msb_parameters["default_granularity"] = self.dataset_metadata.get("default_granularity")
        self.is_period_table = self.dataset_metadata.get("misc_info", {}).get("uses_period_calendar")

        msb_parameters["con"] = Connector("db", database_id=database_id,
                                          sql_dialect=self.dataset_metadata.get("sql_dialect"),
                                          limit=self.sql_row_limit)

        _, msb_parameters["dim_hierarchy"] = self.sp.data.get_dimension_hierarchy()
        msb_parameters["constrained_values"] = self.constrained_values

        ## Map Env Variables

        # Get metric_props, dim_props, setting on env since the chart templates reference these
        env.metric_props = self.get_metric_props()
        env.dim_props = self.get_dimension_props()

        # Get metrics and metric pills, set metric if none were provided.

        if not env.metric:

            share_metrics = [met_name for met_name, met_props in env.metric_props.items() if
                             met_props.get("metric_type") == "share"]

            if share_metrics:
                env.metric = share_metrics[0]

        # Get metrics and metric pills
        msb_parameters["metric"] = env.metric
        msb_parameters["impact_calcs"] = env.impact_calcs

        metric_pills = self.get_metric_pills(env.metric, env.metric_props)

        # Get filters by dimension
        msb_parameters["query_filters"], query_filters_pills = self.parse_dimensions(env)
        msb_parameters["market_cols"] = env.market_cols

        # guardrails for unsupported calculated filters
        calculated_metric_filters = env.calculated_metric_filters if hasattr(env, "calculated_metric_filters") else None
        query, llm_notes, _, _ = self.get_metric_computation_filters([env.metric], calculated_metric_filters, "None",
                                                                     env.metric_props)
        if query:
            self.get_unsupported_filter_message(llm_notes, 'market share analysis')

        ## Period Handling
        compare_date_warning_msg = None
        # TODO make skill work without period table

        # switch between period table and non-period table handling
        time_variable = env.periods if hasattr(env, "periods") else env.time_extract if hasattr(env,
                                                                                                "time_extract") else None
        if not time_variable:
            exit_with_status("A time variable must be provided. It needs to be called 'periods' or 'time_extract'.")

        if not self.is_period_table:
            periods_in_year = self.get_periods_in_year(sp=self.sp)
            msb_parameters["periods_in_year"] = periods_in_year
            start_date, end_date, comp_start_date, comp_end_date = self.handle_periods_and_comparison_periods(
                time_variable, env.growth_type, allowed_tokens=['<no_period_provided>', '<since_launch>'])

            # date/period column metadata. Assumes the date column is a date type
            period_col = env.date_col if hasattr(env, "date_col") and env.date_col else self.get_period_col()

            if not period_col:
                exit_with_status("A date column must be provided.")

            # create period filters using start date and end date, and comparison start and end dates
            period_filters = []

            if start_date and end_date:
                period_filters.append(
                    {"col": period_col, "op": "BETWEEN", "val": f"'{start_date}' AND '{end_date}'"}
                )

            if comp_start_date and comp_end_date:
                period_filters.append(
                    {"col": period_col, "op": "BETWEEN", "val": f"'{comp_start_date}' AND '{comp_end_date}'"}
                )
                if self.is_date_range_completely_out_of_bounds(comp_start_date, comp_end_date):
                    time_granularity = self.dataset_metadata.get("default_granularity")
                    start_period = self.helper.format_date_from_time_granularity(self.dataset_metadata["min_date"],
                                                                                 time_granularity)
                    end_period = self.helper.format_date_from_time_granularity(self.dataset_metadata["max_date"],
                                                                               time_granularity)
                    msg = [
                        f"Please inform the user that the analysis cannot run because data is unavailable for the required {env.growth_type} comparison period."]
                    msg.append(f"Data is only available from {start_period} to {end_period}.")
                    msg.append(
                        f"Ask the user to modify the date range to ensure it aligns with an available {env.growth_type} comparison period within this timeframe.")
                    msg.append("Please do not make any assumptions on behalf of the user.")
                    exit_with_status(" ".join(msg))
                elif self.is_date_range_partially_out_of_bounds(comp_start_date, comp_end_date):
                    compare_date_warning_msg = "Data is only avaiable for partial comparison period. This gap might impact the analysis results and insights."

            # format dates after adding them to the period filters

            start_date = self.helper.format_date_from_time_granularity(start_date,
                                                                       msb_parameters["default_granularity"])
            end_date = self.helper.format_date_from_time_granularity(end_date, msb_parameters["default_granularity"])
            comp_start_date = self.helper.format_date_from_time_granularity(comp_start_date,
                                                                            msb_parameters["default_granularity"])
            comp_end_date = self.helper.format_date_from_time_granularity(comp_end_date,
                                                                          msb_parameters["default_granularity"])

            date_labels = {"start_date": start_date, "end_date": end_date, "compare_start_date": comp_start_date,
                           "compare_end_date": comp_end_date}
        else:
            periods_in_year = self.get_periods_in_year(sp=self.sp)
            msb_parameters["periods_in_year"] = periods_in_year

            date_filters, _ = self.get_time_variables(time_variable)
            period_filters, date_labels = self.get_period_filters(sql_con=msb_parameters["con"],
                                                                  date_filters=date_filters,
                                                                  growth_type=env.growth_type,
                                                                  sparkline_n_year=2,
                                                                  convert_granularity=msb_parameters[
                                                                      "default_granularity"])

            if str(env.growth_type).lower() != "none" and not date_labels.get("compare_start_date"):
                msg = [
                    f"Please inform the user that the analysis cannot run because data is unavailable for the required {env.growth_type} comparison period."]
                msg.append(
                    f"Data is only available from {date_labels.get('data_start_date')} to {date_labels.get('data_end_date')}.")
                msg.append(
                    f"Ask the user to modify the date range to ensure it aligns with an available {env.growth_type} comparison period within this timeframe.")
                msg.append("Please do not make any assumptions on behalf of the user.")
                exit_with_status(" ".join(msg))
            elif self.is_period_date_partially_out_of_bounds(period_filters):
                compare_date_warning_msg = "Data is only avaiable for partial comparison period. This gap might impact the analysis results and insights."

            print("period_filters")
            print(period_filters)

            start_date = date_labels.get("start_date")
            end_date = date_labels.get("end_date")
            comp_start_date = date_labels.get("compare_start_date")
            comp_end_date = date_labels.get("compare_end_date")

        # Set the msb date parameters
        msb_parameters["period_filters"] = period_filters
        msb_parameters["date_labels"] = date_labels
        msb_parameters["compare_date_warning_msg"] = compare_date_warning_msg

        # convert limit_n to an int
        if hasattr(env, "limit_n") and env.limit_n:
            if env.limit_n == NO_LIMIT_N:
                msb_parameters["limit_n"] = None
            else:
                msb_parameters["limit_n"] = self.convert_to_int(env.limit_n)

        # custom skill parameters
        msb_parameters["growth_type"] = env.growth_type

        if str(env.include_drivers).lower() == "true":
            msb_parameters["include_drivers"] = True
        else:
            msb_parameters["include_drivers"] = False

        print("env.growth_type", env.growth_type)
        ## add UI bubbles

        if metric_pills:
            pills["metrics"] = f"Metrics: {self.helper.and_comma_join(metric_pills)}"
        if query_filters_pills:
            pills["filters"] = f"Filter: {self.helper.and_comma_join(query_filters_pills)}"
        if start_date == end_date:
            pills["period"] = f"Period: {start_date}"
        else:
            pills["period"] = f"Period: {start_date} to {end_date}"
        if comp_start_date == comp_end_date:
            pills["compare_period"] = f"Compare Period: {comp_start_date}"
        else:
            pills["compare_period"] = f"Compare Period: {comp_start_date} to {comp_end_date}"
        if env.growth_type and env.growth_type != "None":
            pills["growth_type"] = f"Growth Type: {env.growth_type}"
        if msb_parameters.get("limit_n"):
            pills["limit_n"] = f"Top {msb_parameters['limit_n']}"

        msb_parameters["ParameterDisplayDescription"] = pills

        ## Set the msb parameters
        env.msb_parameters = msb_parameters
