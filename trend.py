from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillVisualization, skill, SkillParameter, SkillInput, SkillOutput, SuggestedQuestion, ParameterDisplayDescription
from skill_framework.preview import preview_skill

from ar_analytics.trend import AdvanceTrend, TrendTemplateParameterSetup
from ar_analytics import ArUtils

import jinja2
import logging

RUNNING_LOCALLY = False

logger = logging.getLogger(__name__)

@skill(
    name="trend",
    description="""Trend Analysis is useful in understanding how metrics have evolved historically across multiple time periods and for observing patterns among different subjects or dimensional categories. It can show multiple metrics side-by-side over multiple time periods. Use this skill if a time period breakout is requested. Do not select this time period if a single period of analysis is selected, even if that request is for growth of that single period. It does not show metrics for single time periods and it does not forecast. For single-point growth questions e.g. "what was [filter] growth in [period]", use the dimension breakout skill to analyze within the relevant dimension.""",
    parameters=[
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods. If given a range, for example 'last 3 quarters, 'between q3 2022 to q4 2023' etc, enumerate the range into a list of valid dates. Don't include natural language words or phrases, only valid dates like 'q3 2023', '2022', 'mar 2020', 'ytd sep 2021', 'mat q4 2021', 'ytd q1 2022', 'ytd 2021', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>' etc."
        ),
        SkillParameter(
            name="metrics",
            is_multi=True,
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
            name="time_granularity",
            is_multi=False,
            constrained_to="date_dimensions",
            description="time granularity provided by the user. only add if explicitly stated by user."
        ),
        SkillParameter(
            name="growth_type",
            constrained_to=None,
            constrained_values=["Y/Y", "P/P", "None"],
            description="Growth type either Y/Y, P/P, or None"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        )
    ]
)
def trend(parameters: SkillInput):
    print(f"Skill received following parameters: {parameters.arguments}")
    param_dict = {"periods": [], "metrics": None, "limit_n": 10, "breakouts": [], "growth_type": None, "other_filters": [], "time_granularity": None}
    
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    env = SimpleNamespace(**param_dict)
    TrendTemplateParameterSetup(env=env)
    env.trend = AdvanceTrend.from_env(env=env)
    df = env.trend.run_from_env()
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.trend.paramater_display_infomation.items()]
    charts = env.trend.default_chart
    tables = [env.trend.display_dfs.get("Metrics Table")]

    insights_dfs = [env.trend.df_notes, env.trend.facts, env.trend.top_facts, env.trend.bottom_facts]

    viz, insights, final_prompt = render_layout(charts,
                                                [tables],
                                                env.trend.title,
                                                env.trend.subtitle,
                                                insights_dfs,
                                                env.trend.warning_message)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=insights,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[]
    )


MAX_PROMPT = """
Answer user question in 30 words or less using following facts: {{facts}}
"""

INSIGHT_PROMPT = """
Write a short headline followed by a 60 word or less paragraph about using facts below.
Use the structure from the 2 examples below to learn how I typically write summary.
Base your summary solely on the provided facts, avoiding assumptions or judgments.
Ensure clarity and accuracy.
Use markdown formatting for a structured and clear presentation.
###
Please use the following as an example of good insights
Example 1:
Facts:
[{'title': 'Breakout facts', 'facts': [{'dim': 'brand', 'dim_member': 'PRIVATE LABEL', 'sales (Current)': 606079483.0, 'sales (Change %)': '+11.2%'}, {'dim': 'brand', 'dim_member': 'BARILLA', 'sales (Current)': 570349013.0, 'sales (Change %)': '+9.8%'}, {'dim': 'brand', 'dim_member': 'GIOVANNI RANA', 'sales (Current)': 171591549.0, 'sales (Change %)': '+45.6%'}, {'dim': 'brand', 'dim_member': 'BUITONI', 'sales (Current)': 132311071.0, 'sales (Change %)': '-0.5%'}, {'dim': 'brand', 'dim_member': 'RONZONI', 'sales (Current)': 118020517.0, 'sales (Change %)': '+20.9%'}, {'dim': 'brand', 'dim_member': "MUELLER'S", 'sales (Current)': 73042850.0, 'sales (Change %)': '+20.5%'}, {'dim': 'brand', 'dim_member': 'DE CECCO', 'sales (Current)': 62208707.0, 'sales (Change %)': '+27.8%'}, {'dim': 'brand', 'dim_member': 'CREAMETTE', 'sales (Current)': 54239556.0, 'sales (Change %)': '+21.0%'}, {'dim': 'brand', 'dim_member': 'SKINNER', 'sales (Current)': 31644679.0, 'sales (Change %)': '+23.0%'}, {'dim': 'brand', 'dim_member': 'SAN GIORGIO', 'sales (Current)': 30549491.0, 'sales (Change %)': '+14.0%'}, {'dim': 'category', 'dim_member': 'PASTA', 'sales (Current)': 2344870759.0, 'sales (Change %)': '+16.9%'}, {'dim': 'segment', 'dim_member': 'SHORT CUT', 'sales (Current)': 799836059.0, 'sales (Change %)': '+16.0%'}, {'dim': 'segment', 'dim_member': 'LONG CUT', 'sales (Current)': 798770283.0, 'sales (Change %)': '+14.1%'}, {'dim': 'segment', 'dim_member': 'FILLED PASTA', 'sales (Current)': 568546418.0, 'sales (Change %)': '+21.2%'}, {'dim': 'segment', 'dim_member': 'BAKING', 'sales (Current)': 117811950.0, 'sales (Change %)': '+13.2%'}, {'dim': 'segment', 'dim_member': 'SOUP CUT', 'sales (Current)': 39237770.0, 'sales (Change %)': '+20.7%'}, {'dim': 'segment', 'dim_member': 'REMAINING FORM', 'sales (Current)': 20666912.0, 'sales (Change %)': '+82.2%'}]}].
    summary:
**Market "Pasta Sales Analysis: Private Label and Filled Pasta Lead Top 10 Brands and Segments"**
Private Label leads with $606M in sales (+11.2%), while Giovanni Rana sees a substantial 45.6% growth. Filled Pasta emerges as the leading growth segment with a 21.2% increase, and overall pasta sales rise by 16.9%.
Example 2:
Facts:
[{'title': 'Breakout facts', 'facts': [{'dim': 'brand', 'dim_member': 'PRIVATE LABEL', 'sales (Current)': 606079483.0, 'sales (Change %)': '+11.2%', 'volume (Current)': 514359980.0, 'volume (Change %)': '+6.8%', 'units (Current)': 456458939.0, 'units (Change %)': '+9.3%'}, {'dim': 'brand', 'dim_member': 'BARILLA', 'sales (Current)': 570349013.0, 'sales (Change %)': '+9.8%', 'volume (Current)': 353012269.0, 'volume (Change %)': '+5.1%', 'units (Current)': 345124705.0, 'units (Change %)': '+3.4%'}, {'dim': 'brand', 'dim_member': 'GIOVANNI RANA', 'sales (Current)': 171591549.0, 'sales (Change %)': '+45.6%', 'volume (Current)': 27993960.0, 'volume (Change %)': '+39.7%', 'units (Current)': 33071740.0, 'units (Change %)': '+34.4%'}, {'dim': 'brand', 'dim_member': 'BUITONI', 'sales (Current)': 132311071.0, 'sales (Change %)': '-0.5%', 'volume (Current)': 22462430.0, 'volume (Change %)': '-2.3%', 'units (Current)': 25485910.0, 'units (Change %)': '-2.8%'}, {'dim': 'brand', 'dim_member': 'RONZONI', 'sales (Current)': 118020517.0, 'sales (Change %)': '+20.9%', 'volume (Current)': 85090257.0, 'volume (Change %)': '+11.8%', 'units (Current)': 91945822.0, 'units (Change %)': '+11.7%'}, {'dim': 'brand', 'dim_member': "MUELLER'S", 'sales (Current)': 73042850.0, 'sales (Change %)': '+20.5%', 'volume (Current)': 52155766.0, 'volume (Change %)': '+13.3%', 'units (Current)': 52357294.0, 'units (Change %)': '+13.1%'}, {'dim': 'brand', 'dim_member': 'DE CECCO', 'sales (Current)': 62208707.0, 'sales (Change %)': '+27.8%', 'volume (Current)': 25002623.0, 'volume (Change %)': '+19.3%', 'units (Current)': 25338725.0, 'units (Change %)': '+18.9%'}, {'dim': 'brand', 'dim_member': 'CREAMETTE', 'sales (Current)': 54239556.0, 'sales (Change %)': '+21.0%', 'volume (Current)': 43253300.0, 'volume (Change %)': '+17.7%', 'units (Current)': 42945964.0, 'units (Change %)': '+17.4%'}, {'dim': 'brand', 'dim_member': 'SKINNER', 'sales (Current)': 31644679.0, 'sales (Change %)': '+23.0%', 'volume (Current)': 22399717.0, 'volume (Change %)': '+22.3%', 'units (Current)': 24109793.0, 'units (Change %)': '+21.5%'}, {'dim': 'brand', 'dim_member': 'SAN GIORGIO', 'sales (Current)': 30549491.0, 'sales (Change %)': '+14.0%', 'volume (Current)': 24264189.0, 'volume (Change %)': '+7.8%', 'units (Current)': 24971083.0, 'units (Change %)': '+7.8%'}, {'dim': 'category', 'dim_member': 'PASTA', 'sales (Current)': 2344870759.0, 'sales (Change %)': '+16.9%', 'volume (Current)': 1381341421.0, 'volume (Change %)': '+9.0%', 'units (Current)': 1388502031.0, 'units (Change %)': '+9.4%'}, {'dim': 'segment', 'dim_member': 'SHORT CUT', 'sales (Current)': 799836059.0, 'sales (Change %)': '+16.0%', 'volume (Current)': 581461232.0, 'volume (Change %)': '+10.2%', 'units (Current)': 598039068.0, 'units (Change %)': '+9.9%'}, {'dim': 'segment', 'dim_member': 'LONG CUT', 'sales (Current)': 798770283.0, 'sales (Change %)': '+14.1%', 'volume (Current)': 585163253.0, 'volume (Change %)': '+6.0%', 'units (Current)': 576367344.0, 'units (Change %)': '+7.7%'}, {'dim': 'segment', 'dim_member': 'FILLED PASTA', 'sales (Current)': 568546418.0, 'sales (Change %)': '+21.2%', 'volume (Current)': 128028938.0, 'volume (Change %)': '+13.8%', 'units (Current)': 119584833.0, 'units (Change %)': '+14.4%'}, {'dim': 'segment', 'dim_member': 'BAKING', 'sales (Current)': 117811950.0, 'sales (Change %)': '+13.2%', 'volume (Current)': 48733961.0, 'volume (Change %)': '+10.8%', 'units (Current)': 57928261.0, 'units (Change %)': '+10.2%'}, {'dim': 'segment', 'dim_member': 'SOUP CUT', 'sales (Current)': 39237770.0, 'sales (Change %)': '+20.7%', 'volume (Current)': 26220794.0, 'volume (Change %)': '+11.3%', 'units (Current)': 32006671.0, 'units (Change %)': '+9.0%'}, {'dim': 'segment', 'dim_member': 'REMAINING FORM', 'sales (Current)': 20666912.0, 'sales (Change %)': '+82.2%', 'volume (Current)': 11732954.0, 'volume (Change %)': '+50.4%', 'units (Current)': 4575565.0, 'units (Change %)': '+80.4%'}]}].
Insights:
**Barilla Performance Analysis: Private Label Tops Sales, Giovanni Rana and Filled Pasta Segment Register Strongest Growth"**
Private Label leads with $606M in sales (+11.2%), 514M in volume (+6.8%), and 456M units (+9.3%). Giovanni Rana shows exceptional growth at 45.6% in sales, 39.7% in volume, and 34.4% in units. Buitoni, however, faces a decline with a -0.5% drop in sales, -2.3% in volume, and -2.8% in units.
In segments, Filled Pasta leads in growth with sales up 21.2%, volume increasing by 13.8%, and units by 14.4%. The Remaining Form segment shows a staggering 82.2% jump in sales, although from a smaller base. Overall, pasta sales in the category rose by 16.9%, with a 9.0% increase in volume and a 9.4% boost in units.
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
                    "color": "#888",
                    "fontFamily": "Arial, sans-serif",
                    "backgroundColor": "#FFF8E1",
                    "borderRadius": "10px"
                },
                "text": "{{warnings}}"
        },
    {% endif %}
    {% for df in dfs %}
        {
        "type": "HighchartsChart",
        "row": {{ns.counter + chart_start}},
        "column": 1,
        "width": 158,
        "height": {{height}},
        "options": {{df}}
    }
    {% if not loop.last %},{% endif %}
    {% set ns.counter = height*loop.index %}
    {% endfor %}
]
}
"""

TABLE_TEMPLATE = """
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
                    "color": "#888",
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
                {% elif loop.index0 == 0 %}
                    {"name": "{{ col }}", "style": {"textAlign": "left", "white-space": "pre"}},
                {% else %}
                    {"name": "{{ col }}"},
                {% endif %}
            {% endfor %}
        ],
        "data": {{ df.fillna('N/A').to_numpy().tolist() | tojson }},
        "styles": {
                    "alternateRowColor": "#f9f9f9",
                    "fontFamily": "Arial, sans-serif",
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

def render_layout(charts, tables, title, subtitle, insights_dfs, warnings):
    DEFAULT_HEIGHT = 80
    template = jinja2.Template(TEMPLATE)
    table_template = jinja2.Template(TABLE_TEMPLATE)
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(INSIGHT_PROMPT).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(MAX_PROMPT).render(**{"facts": facts})
    insights = insight_template

    viz = []
    for name, chart in charts.items():
        if name.strip().startswith("·"):
            name = name.replace("·", "").strip()
        height = (DEFAULT_HEIGHT // len(chart)) + 1
        template_vars = {
            'dfs': chart,
            "height": height,
            "title": title,
            "subtitle": subtitle,
            "warnings": warnings
        }
        rendered = template.render(**template_vars)
        viz.append(SkillVisualization(title=name, layout=rendered))


    table_template_vars = {
        'dfs': tables[0],
        "height": DEFAULT_HEIGHT,
        "title": title,
        "subtitle": subtitle,
        "warnings": warnings
    }
    table = table_template.render(**table_template_vars)
    viz.append(SkillVisualization(title="Metrics Table", layout=table))

    # adding insights
    ar_utils = ArUtils()
    rendered_insight = ar_utils.get_llm_response(insights)

    return viz, rendered_insight, max_response_prompt

if __name__ == '__main__':
    skill_input: SkillInput = trend.create_input(arguments={'metrics': ["sales", "volume", "sales_share", "volume_share"], 'periods': ["mat jun 2021"], "other_filters": [{"dim": "brand", "op": "=", "val": ["barilla"]}]})
    out = trend(skill_input)
    preview_skill(trend, out)