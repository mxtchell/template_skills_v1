from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, SuggestedQuestion, \
    ParameterDisplayDescription
from skill_framework.preview import preview_skill

from market_share_breakdown import MarketShareBreakdown, MSBTemplateParameterSetup
from ar_analytics import ArUtils

import jinja2
import logging
import uuid

logger = logging.getLogger(__name__)


@skill(
    name="Market_Share_Analysis",
    description="Use this skill to analyze the drivers of a subject's share.The drivers are derived from the available metrics and the dimensions in the dataset. If a time period is one of the breakouts, this may not be the correct skill.",
    parameters=[
        SkillParameter(
            name="metric",
            constrained_to="metrics",
            is_multi=False
        ),
        SkillParameter(
            name="growth_type",
            is_multi=False,
            constrained_values=["Y/Y", "P/P"],
            description="Growth type either Y/Y or P/P"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="limit_n",
            description="limit the number of values by this number"
        ),
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods. If given a range, for example 'last 3 quarters, 'between q3 2022 to q4 2023' etc, enumerate the range into a list of valid dates. Don't include natural language words or phrases, only valid dates like 'q3 2023', '2022', 'mar 2020', 'ytd sep 2021', 'mat q4 2021', 'ytd q1 2022', 'ytd 2021', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>' etc."
        )
    ]
)
def market_share_analysis(parameters: SkillInput):
    print(f"Skill received following parameters: {parameters}")
    param_dict = {"periods": [], "metric": None, "limit_n": 20, "growth_type": "Y/Y", "other_filters": [], "global_view": [
    {
        "dim": "sub_category",
        "type": "share",
        "tab_label": "Sub Category"
    },
    {
        "dim": "state_name",
        "type": "share",
        "tab_label": "State"
    }
], "market_view": [
    {
        "dim": "brand",
        "type": "contribution",
        "tab_label": "Portfolio",
        "drilldown":
        {
            "dim": "base_size",
            "type": "contribution"
        }
    },
    {
        "dim": "segment",
        "type": "share",
        "tab_label": "Segments"
    }
], "include_drivers": True, "market_cols": ["sub_category", "state_name"], "impact_calcs": {},
                  "decomposition_display_config": {  "Impact on Share": {
        "sales_share": ["impact"],
        "volume_share": ["impact"]
    },
    "Metric Decomposition": {
        "sales": ["pct_change"],
        "volume": ["pct_change"],
        "tdp": ["pct_change"]
    }
}, "subject_metric_config": {
    "Metric Relationship": {
        "sales": ["pct_change"],
        "volume": ["current", "pct_change"],
        "tdp": ["pct_change"]
    }
}
}
    print(f"Skill received following parameters: {parameters.arguments}")
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    if str(param_dict["growth_type"]).lower() not in ["y/y", 'p/p']:
        param_dict["growth_type"] = "Y/Y"

    env = SimpleNamespace(**param_dict)
    MSBTemplateParameterSetup(env=env)
    env.msa = MarketShareBreakdown.from_env(env=env)
    result_dfs = env.msa.run_from_env()
    print(result_dfs.keys())
    tables = env.msa.get_display_tables()
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.msa.paramater_display_infomation.items()]

    insights_dfs = [env.msa.subject_facts, env.msa.bottom_peers_facts, env.msa.top_peers_facts, env.msa.bottom_breakouts_facts, env.msa.top_breakouts_facts, env.msa.metric_driver_challenges_facts, env.msa.df_notes]
    followups = env.msa.suggestions

    viz, insights, final_prompt = render_layout(tables, env.msa.title, env.msa.subtitle, insights_dfs, env.msa.warning_message)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=insights,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[SuggestedQuestion(label=f.get("label"), question=f.get("question")) for f in followups if
                            f.get("label")]
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

if __name__ == '__main__':
    skill_input: SkillInput = market_share_analysis.create_input(
        arguments={'metric': "sales", 'periods': ["2022"], 'other_filters': [{"val": ["barilla"],"dim": "brand","op": "="},  {
      "val": [
        "semolina"
      ],
      "dim": "sub_category",
      "op": "="
    }]})
    out = market_share_analysis(skill_input)
    preview_skill(market_share_analysis, out)