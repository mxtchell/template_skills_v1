from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, SuggestedQuestion, \
    ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData

from ar_analytics import MarketShareBreakdown, MSBTemplateParameterSetup, ArUtils
from ar_analytics.defaults import market_share_analysis_config

import jinja2
import logging

logger = logging.getLogger(__name__)

@skill(
    name=market_share_analysis_config.name,
    llm_name=market_share_analysis_config.llm_name,
    description=market_share_analysis_config.description,
    capabilities=market_share_analysis_config.capabilities,
    limitations=market_share_analysis_config.limitations,
    example_questions=market_share_analysis_config.example_questions,
    parameter_guidance=market_share_analysis_config.parameter_guidance,
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
            description="Growth type either Y/Y or P/P",
            default_value="Y/Y"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="limit_n",
            description="limit the number of values by this number",
            default_value=20
        ),
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods. If given a range, for example 'last 3 quarters, 'between q3 2022 to q4 2023' etc, enumerate the range into a list of valid dates. Don't include natural language words or phrases, only valid dates like 'q3 2023', '2022', 'mar 2020', 'ytd sep 2021', 'mat q4 2021', 'ytd q1 2022', 'ytd 2021', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>' etc."
        ),
        # SkillParameter(
        #     name="global_view",
        #     parameter_type="code"
        # ),
        # SkillParameter(
        #     name="market_view",
        #     parameter_type="code"
        # ),
        # SkillParameter(
        #     name="include_drivers",
        #     parameter_type="code",
        #     default_value="True"
        # ),
        # SkillParameter(
        #     name="market_cols",
        #     parameter_type="code"
        # ),
        # SkillParameter(
        #     name="impact_calcs",
        #     parameter_type="code"
        # ),
        # SkillParameter(
        #     name="decomposition_display_config",
        #     parameter_type="code"
        # ),
        # SkillParameter(
        #     name="subject_metric_config",
        #     parameter_type="code"
        # ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=market_share_analysis_config.max_prompt
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=market_share_analysis_config.insight_prompt
        )
    ]
)
def market_share_analysis(parameters: SkillInput):
    print(f"Skill received following parameters: {parameters}")
    param_dict = {"periods": [], "metric": None, "limit_n": 20, "growth_type": "Y/Y", "other_filters": [], "global_view": [], "market_view": [],
                  "include_drivers": True, "market_cols": [], "impact_calcs": {}, "decomposition_display_config": {}, "subject_metric_config": {}}

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

    viz, insights, final_prompt, export_data = render_layout(tables,
                                                            env.msa.title,
                                                            env.msa.subtitle,
                                                            insights_dfs,
                                                            env.msa.warning_message,
                                                            parameters.arguments.max_prompt,
                                                            parameters.arguments.insight_prompt)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=insights,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[SuggestedQuestion(label=f.get("label"), question=f.get("question")) for f in followups if
                            f.get("label")],
        export_data=[ExportData(name=name, data=df) for name, df in export_data.items()]
    )


def render_layout(tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt):
    height = 80
    template = jinja2.Template(TEMPLATE)
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

    for name, table in tables.items():
        export_data[name] = table
        template_vars = {
            'dfs': [table],
            "height": height,
            "title": title,
            "subtitle": subtitle,
            "warnings": warnings
        }
        rendered = template.render(**template_vars)
        viz_list.append(SkillVisualization(title=name, layout=rendered))
    return viz_list, insights, max_response_prompt, export_data

TEMPLATE = """
{
"type": "Document",
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