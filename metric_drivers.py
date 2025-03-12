from __future__ import annotations
from types import SimpleNamespace

import pandas as pd
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, SuggestedQuestion, ParameterDisplayDescription
from skill_framework.preview import preview_skill

from ar_analytics.driver_analysis import DriverAnalysis, DriverAnalysisTemplateParameterSetup
from ar_analytics import ArUtils

import jinja2
import logging
import uuid

logger = logging.getLogger(__name__)

@skill(
    name="metric_drivers",
    description="""
        Utilize this skill to dissect and comprehend the influence of various metrics and dimensions on the performance of a specific metric. 
        This analysis is useful to explain overall performance of a [dim value].
        It is helpful to understand what's driving the changes in metrics. Explanations include driving metrics, changes across different breakouts and comparisons. This skill is designed to address "Why" questions or to provide explanations upon request. 
        If a time period is one of the breakouts, this may not be the correct skill. 
    """,
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
            name="limit_n",
            description="limit the number of values by this number"
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
            description="Growth type either Y/Y or P/P"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="calculated_metric_filters",
            description='This parameter allows filtering based on computed values like growth, delta, or share. The computed values are only available for metrics selected for this analysis. The available computations are growth, delta and share. It accepts a list of conditions, where each condition is a dictionary with:  metric: The metric being filtered. computation: The computation (growth, delta, share) operator: The comparison operator (">", "<", ">=", "<=", "between", "=="). value: The numeric threshold for filtering. If using "between", provide a list [min, max]. scale: the scale of value (percentage, bps, absolute)'
        )
    ]
)
def simple_metric_driver(parameters: SkillInput):
    param_dict = {"periods": [], "metric": None, "limit_n": 10, "breakouts": None, "growth_type": None, "other_filters": [], "calculated_metric_filters": None}
    print(f"Skill received following parameters: {parameters.arguments}")
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    env = SimpleNamespace(**param_dict)
    vars(env).update(vars(parameters.arguments))
    DriverAnalysisTemplateParameterSetup(env=env)
    env.da = DriverAnalysis.from_env(env=env)

    _ = env.da.run_from_env()

    results = env.da.get_display_tables()

    tables = {
        "Metrics": results['viz_metric_df']
    }
    tables.update(results['viz_breakout_dfs'])

    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.da.paramater_display_infomation.items()]

    insights_dfs = [env.da.df_notes, env.da.breakout_facts, env.da.subject_fact.get("df", pd.DataFrame())]
    # followups = env.da.get_suggestions()

    warning_messages = env.da.get_warning_messages()

    viz, insights, final_prompt = render_layout(tables, env.da.title, env.da.subtitle, insights_dfs, warning_messages)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=insights,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[]
        # followup_questions=[SuggestedQuestion(label=f.get("label"), question=f.get("question")) for f in followups if f.get("label")]
    )
    
    
def render_layout(tables, title, subtitle, insights_dfs, warnings):
    height = 80
    template = jinja2.Template(TEMPLATE)
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(INSIGHT_PROMPT).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(MAX_PROMPT).render(**{"facts": facts})

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)
    viz_list = []

    for name, table in tables.items():
        template_vars = {
            'dfs': [table],
            "height": height,
            "title": title,
            "subtitle": subtitle,
            "warnings": warnings
        }
        rendered = template.render(**template_vars)
        viz_list.append(SkillVisualization(title=name, layout=rendered))
    return viz_list, insights, max_response_prompt

MAX_PROMPT = """
"""

INSIGHT_PROMPT = """
{{base_prompt}} Write a short headline followed by a 60 word or less paragraph about using facts below.
Use the structure from the 2 examples below to learn how I typically write summary.
Base your summary solely on the provided facts, avoiding assumptions or judgments.
Ensure clarity and accuracy.
Use markdown formatting for a structured and clear presentation.
###
Please use the following as an example of good insights
Example 1:
Facts:
[{'title': 'Peer Facts', 'facts': [{'level': 'Brand', 'competitor': 'PRIVATE LABEL', 'Sales': '$606,077,860.02', 'change': '$61,256,314.08', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 61256314.07799971}, {'level': 'Brand', 'competitor': 'GIOVANNI RANA', 'Sales': '$171,591,436.24', 'change': '$53,770,879.92', 'rank_curr': '3', 'rank_change': '+1', 'abs_diff': 53770879.92400019}, {'level': 'Brand', 'competitor': 'BARILLA', 'Sales': '$570,348,946.24', 'change': '$50,774,602.17', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 50774602.167000055}, {'level': 'Manufacturer', 'competitor': 'PRIVATE LABEL', 'Sales': '$606,077,860.02', 'change': '$61,256,314.08', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 61256314.07799971}, {'level': 'Manufacturer', 'competitor': 'PASTIFICIO RANA S.P.A.', 'Sales': '$171,591,436.24', 'change': '$53,770,879.92', 'rank_curr': '4', 'rank_change': '+1', 'abs_diff': 53770879.92400019}, {'level': 'Manufacturer', 'competitor': 'BARILLA G & R F.LLI S.P.A.', 'Sales': '$570,445,950.79', 'change': '$50,797,040.32', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 50797040.32000005}]}}, {'title': 'Breakout Facts', 'facts': []}, {'title': 'Subject Facts', 'facts': [{'metric': 'Sales', 'curr': '$2,344,866,829.62', 'prev': '$2,006,377,582.82', 'diff': '$338,489,246.79', 'growth': '16.87%', 'parent_metric': None, 'depth': 0}]}}, {'title': 'Notes about this answer', 'facts': [{'Note to the assistant:': 'The analysis was run using only these filters: Category = pasta'}, {'Note to the assistant:': 'Analysis ran for metric Sales filtered to pasta for Category for the period January 2022 to December 2022 vs January 2021 to December 2021'}]}, {'title': 'Metric Tree Facts', 'facts': [{'metric': 'Sales', 'curr': '$2,344,866,829.62', 'prev': '$2,006,377,582.82', 'diff': '$338,489,246.79', 'growth': '16.87%', 'parent_metric': None, 'depth': 0}, {'metric': 'ACV', 'curr': '3,614,315.41', 'prev': '3,919,522.06', 'diff': '-305,206.66', 'growth': '-7.79%', 'parent_metric': 'Sales', 'depth': 1}, {'metric': 'Volume', 'curr': '1,381,319,805.77', 'prev': '1,267,565,899.76', 'diff': '113,753,906.00', 'growth': '8.97%', 'parent_metric': 'Sales', 'depth': 1}]}]}.
summary:
## Pasta Sales Analysis ##
**Performance Overview:**
The pasta category has experienced significant increase in sales (+16.87% ), rising from $2,006,377,582.82 to $2,344,866,829.62..
**Driving Metrics:**
Sales growth was driven by volume increase (+8.79%), while ACV declined by 7.79%, from 3,919,522.06 to 3,614,315.41.
**Key Drivers:**
PRIVATE LABEL led the market with $606,077,860.02 in sales, up by $61,256,314.08. GIOVANNI RANA and BARILLA followed, with sales increases of $53,770,879.92 and $50,774,602.17, respectively.
###
Example 2:
Facts:
[{'title': 'Peer Facts', 'facts': [{'level': 'Brand', 'competitor': 'PRIVATE LABEL', 'Sales': '$152,807,608.00', 'change': '-$453,271,875.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 453271875.0}, {'level': 'Brand', 'competitor': 'BARILLA', 'Sales': '$146,567,322.00', 'change': '-$423,781,691.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 423781691.0}, {'level': 'Brand', 'competitor': 'GIOVANNI RANA', 'Sales': '$41,702,602.00', 'change': '-$129,888,947.00', 'rank_curr': '3', 'rank_change': 'No Change', 'abs_diff': 129888947.0}]}}, {'title': 'Breakout Facts', 'facts': [{'level': 'Base Size', 'driver': '16 OUNCE', 'Sales': '$101,274,375.00', 'change': '-$306,205,018.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 306205018.0}, {'level': 'Base Size', 'driver': '12 OUNCE', 'Sales': '$24,372,801.00', 'change': '-$61,157,379.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 61157379.0}, {'level': 'Base Size', 'driver': '14.5 OUNCE', 'Sales': '$7,007,526.00', 'change': '-$16,631,014.00', 'rank_curr': '3', 'rank_change': 'No Change', 'abs_diff': 16631014.0}, {'level': 'Manufacturer', 'driver': 'BARILLA G & R F.LLI S.P.A.', 'Sales': '$146,567,322.00', 'change': '-$423,781,691.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 423781691.0}, {'level': 'Segment', 'driver': 'SHORT CUT', 'Sales': '$61,985,296.00', 'change': '-$185,709,862.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 185709862.0}, {'level': 'Segment', 'driver': 'LONG CUT', 'Sales': '$61,464,163.00', 'change': '-$177,343,077.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 177343077.0}, {'level': 'Segment', 'driver': 'BAKING', 'Sales': '$12,497,475.00', 'change': '-$34,233,200.00', 'rank_curr': '3', 'rank_change': 'No Change', 'abs_diff': 34233200.0}, {'level': 'Sub-Category', 'driver': 'SEMOLINA', 'Sales': '$117,014,563.00', 'change': '-$343,972,411.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 343972411.0}, {'level': 'Sub-Category', 'driver': 'MULTIGRAIN', 'Sales': '$12,526,184.00', 'change': '-$31,841,841.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 31841841.0}, {'level': 'Sub-Category', 'driver': 'REMAINING GRAIN', 'Sales': '$6,304,828.00', 'change': '-$18,091,439.00', 'rank_curr': '4', 'rank_change': '-1', 'abs_diff': 18091439.0}]}}, {'title': 'Subject Facts', 'facts': [{'metric': 'Sales', 'curr': '$146,567,322.00', 'prev': '$570,349,013.00', 'diff': '-$423,781,691.00', 'growth': '-74.30%', 'parent_metric': None, 'depth': 0}]}}, {'title': 'Notes about this answer', 'facts': [{'Note to the assistant:': 'The analysis was run using only these filters: Brand = barilla, Category = pasta, Country = united states'}]}, {'title': 'Metric Tree Facts', 'facts': [{'metric': 'Sales', 'curr': '$146,567,322.00', 'prev': '$570,349,013.00', 'diff': '-$423,781,691.00', 'growth': '-74.30%', 'parent_metric': None, 'depth': 0}, {'metric': 'Volume', 'curr': '90,622,621.00', 'prev': '353,012,269.00', 'diff': '-262,389,648.00', 'growth': '-74.33%', 'parent_metric': 'Sales', 'depth': 1}, {'metric': 'Units', 'curr': '89,526,220.00', 'prev': '345,124,705.00', 'diff': '-255,598,485.00', 'growth': '-74.06%', 'parent_metric': 'Sales', 'depth': 1}]}]}.
summary:
## Barilla Sales Analysis ##
**Performance Overview:**
Barilla's sales witnessed a -74.30% decline, dropping from $570,349,013.00 to $146,567,322.00. This contraction is slightly below the benchmark set by competitors like PRIVATE LABEL and GIOVANNI RANA,
**Driving Metrics:**
Sales for Barilla plummeted by -74.30%, driven by substantial declines in volume (-74.33%, from 353,012,269 to 90,622,621) and units (-74.06%, from 345,124,705 to 89,526,220).
**Key Drivers:**
Base Size adjustments with the 16 OUNCE package experiencing the most significant sales drop of $306,205,018.00.
Manufacturer insights reveal Barilla G & R F.LLI S.P.A. as the most affected, paralleling the brand's own sales contraction.
Segment analysis points out the SHORT CUT and LONG CUT as leading categories in sales decline, with SHORT CUT experiencing a $185,709,862.00 drop.
Sub-Category trends show SEMOLINA and MULTIGRAIN as heavily impacted, with SEMOLINA sales down by $343,972,411.00.
###
Facts:
{{facts}}
Summary:
"""


TEMPLATE = """
{
"type": "GridPanel",
"rows": 100,
"columns": 160,
"rowHeight": "1.11%",
"colWidth": "0.625%",
"gap": "0px",
"style": {
    "backgroundColor": "white",
    "border": "1px solid #ccc",
    "width": "100%",
    "height": "100%"
},
 "children": [
    {% set ns = namespace(counter=0) %}
    {
            "name": "mainTitle",
            "type": "Header",
            "row": 0,
            "column": 1,
            "width": 120,
            "height": 2,
            "style": {
                "textAlign": "left",
                "verticalAlign": "middle",
                "fontSize": "18px",
                "fontWeight": "bold",
                "color": "#333",
                "fontFamily": "Arial, sans-serif"
            },
            "text": "{{title}}"
    },
    {
            "name": "subtitle",
            "type": "Header",
            "row": 4,
            "column": 1,
            "width": 120,
            "height": 2,
            "style": {
                "textAlign": "left",
                "verticalAlign": "middle",
                "fontSize": "12px",
                "color": "#888",
                "fontFamily": "Arial, sans-serif"
            },
            "text": "{{subtitle}}"
    },
    {% set chart_start = 7 %}
    {% if warnings %}
        {% set chart_start = 10 %}
        {
                "name": "subtitle",
                "type": "Header",
                "row": 7,
                "column": 1,
                "width": 158,
                "height": 2,
                "style": {
                    "textAlign": "left",
                    "verticalAlign": "middle",
                    "color": "#333",
                    "fontFamily": "Arial, sans-serif",
                    "backgroundColor": "#FFF8E1",
                    "borderRadius": "10px"
                },
                "text": "{{warnings}}"
        },
    {% endif %}
    {% for df in dfs %}
        {
        "type": "DataTable",
        "row": {{ns.counter + chart_start}},
        "column": 1,
        "width": 158,
        "height": {{height}},
        "columns": [
            {% set total_cols = df.columns | length  %}
            {% for col in df.columns %}
                {% if loop.index0 == (df.columns | length) - 1 %}
                    {"name": "{{ col }}"}
                {% else %}
                    {"name": "{{ col }}"},
                {% endif %}
            {% endfor %}
        ],
        "data": {{ df.fillna('N/A').to_numpy().tolist() | tojson }},
        "styles": {
                    "alternateRowColor": "#f9f9f9",
                    "fontFamily": "Arial, sans-serif",
                    "td": {
                      "white-space": "pre",
                      "textAlign": "left"
                    },
                    "th": {
                        "backgroundColor": "#FOFOFO",
                        "color": "#000000",
                        "fontWeight": "bold"
                    },
                    "caption": {
                        "backgroundColor": "#32ea05",
                        "color": "#000000",
                        "fontWeight": "bold",
                        "fontSize": "10pt"
                    }
        }
    }{% if not loop.last %},{% endif %}
    {% set ns.counter = height*loop.index %}
    {% endfor %}
]
}
"""

if __name__ == '__main__':
    skill_input: SkillInput = simple_metric_driver.create_input(arguments={'metric': "sales", 'breakouts': ["brand", "manufacturer"],'limit_n': 10, 'periods': ["2023"], 'growth_type': "Y/Y", 'other_filters': []})
    out = simple_metric_driver(skill_input)
    preview_skill(simple_metric_driver, out)