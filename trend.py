from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillVisualization, skill, SkillParameter, SkillInput, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData

from ar_analytics import AdvanceTrend, TrendTemplateParameterSetup, ArUtils
from ar_analytics.defaults import trend_analysis_config

import jinja2
import logging

RUNNING_LOCALLY = False

logger = logging.getLogger(__name__)

@skill(
    name=trend_analysis_config.name,
    llm_name=trend_analysis_config.llm_name,
    description=trend_analysis_config.description,
    capabilities=trend_analysis_config.capabilities,
    limitations=trend_analysis_config.limitations,
    example_questions=trend_analysis_config.example_questions,
    parameter_guidance=trend_analysis_config.parameter_guidance,
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
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=trend_analysis_config.max_prompt
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=trend_analysis_config.insight_prompt
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
                                                env.trend.warning_message,
                                                parameters.arguments.max_prompt,
                                                parameters.arguments.insight_prompt)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=insights,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[],
        export_data=[ExportData(name="Metrics Table", data=tables[0])]
    )

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

def render_layout(charts, tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt):
    DEFAULT_HEIGHT = 80
    template = jinja2.Template(TEMPLATE)
    table_template = jinja2.Template(TABLE_TEMPLATE)
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})
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