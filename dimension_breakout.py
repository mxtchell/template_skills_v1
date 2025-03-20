from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, SuggestedQuestion, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData

from ar_analytics import BreakoutAnalysis, BreakoutAnalysisTemplateParameterSetup, defaults, ArUtils

import jinja2
import logging

logger = logging.getLogger(__name__)

@skill(
    name="Dimension Breakout",
    llm_name="dimension_breakout",
    description="""The breakout skill is designed to enable comparisons and analyses of subjects that belong to the same group to show how these subjects rank or differ from one another. 
This skill also provides multiple KPIs across a subject. 
This skill should answer questions without breakouts, like a single value. 
If a time period is one of the breakouts, this may not be the correct skill. However, if a single time period is asked for this likely is the correct skill to choose. 
For single-point growth questions e.g. "what was [filter] growth in[period]", use this skill to analyze within the relevant dimension.""",
    capabilities="""Provides a table based on the the inputed metric and given filters, and can breakout by dimenion(s) for a time period. When broken out, each dimension value is ranked by desired sort.  
Summative metrics only are also shown on bridge chart (decomposition).
Users can have a scalar value return with specific filters, or have one or more breakouts when requested. 
Users can add in additional metrics columns and metric growth columns by request. 
Users can ask for year over year or period over period growth.""",
    limitations="""-Users cannot remove specific columns, they can only add metrics and enable or disable growth metric columns. For example, if a user asks to remove the "Sales change % column" they should be told that is not possible. 
-When users asked to breakout by multiple dimensions, they will get a separate table for each dimension. They will not receive a table that includes both breakouts in one view.
- Users can't see the data on chart, just table
- Users cannot reorder columns
- users can't see the details of share value trended over time.""",
    example_questions="""- How is [filter] performing in each {dimension] vs. competition?
- Which [dimension]s or {dimension]s show the highest growth of [metric]? 
- Which [dimension]s are declining the most?
- What are the highest [metric] [dimension] in [filter] this [period]?
- What is the [metric] of the highest [metric] [dimension]?  
- Which [dimension]s are showing the most growth in [filter]?""",
    parameter_guidance="""<TIME PERIODHANDLING>
- TIME PERIOD HANDLING: Use this section to better understand time periods for the 'periods' parameter selection: 
  - today is {{today}}. 
  - The data ends on {{copilot_dataset_end_date}}.
  -the latest period is the most recent full period in relation to the end of the data on {{copilot_dataset_end_date}}.
  - Phrases such as 'YTD', 'ytd', and 'this year' phrases result in time period analyses that end with the last date in the data. Prioritize the user's time period request when given. 
  -Phrases such as 'last X months' result in a time period analysis of X number of months of data ending with the last date in the data. 
  -For phrases requesting to compare 2 years or time periods, such as 'YYYY vs. YYYY', use the most recent YYYY as the 'period' parameter and add 'Y/Y' for the 'growth' parameter to show the comparison (vs.) to the previous year. 
  -If the user asks to compare 2 non-consecutive years, let them know you can only analyze consecutive time periods to show year-over-year (Y/Y) or period-over-period (P/P) growth. 
  -For phrases such as 'Last X months vs. last year',  let's break this down into 2 sections. ('last X months') and ('vs. last year'). Use ('last X months') as the time period and use (vs. last year) to add 'Y/Y' for the 'Growth' parameter. 
  - For phrases such as 'annual growth' show the MAT time period and set the 'growth' parameter to 'Y/Y'.
  - for phrases such as 'vs. YA', choose 'Y/Y' for the growth parameter. Set the period parameter to the baseline time period the user wishes to compare to.
- If no time period is provided, execute using skill default.
<TIME PERIODHANDLING>

<BREAKOUT DIMENSION>
include here just categorical dimension. This skill does not breakout by data dimension. 
<BREAKOUT DIMENSION>""",
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
            name="growth_trend",
            constrained_to=None,
            constrained_values=["fastest growing", "highest growing", "highest declining", "fastest declining",
                                "smallest overall", "biggest overall"],
            description="indicates the trend type (fastest, highest, overall size) within a specified growth metric (year over year, period over period) for entities being analyzed."
        ),
        SkillParameter(
            name="calculated_metric_filters",
            description='This parameter allows filtering based on computed values like growth, delta, or share. The computed values are only available for metrics selected for this analysis. The available computations are growth, delta and share. It accepts a list of conditions, where each condition is a dictionary with:  metric: The metric being filtered. computation: The computation (growth, delta, share) operator: The comparison operator (">", "<", ">=", "<=", "between", "=="). value: The numeric threshold for filtering. If using "between", provide a list [min, max]. scale: the scale of value (percentage, bps, absolute)'
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=defaults.default_max_prompt
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=defaults.breakout_insight_prompt
        )
    ]
)
def simple_breakout(parameters: SkillInput):
    param_dict = {"periods": [], "metrics": None, "limit_n": 10, "breakouts": None, "growth_type": None, "other_filters": [], "growth_trend": None, "calculated_metric_filters": None}
    print(f"Skill received following parameters: {parameters.arguments}")
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    env = SimpleNamespace(**param_dict)
    BreakoutAnalysisTemplateParameterSetup(env=env)
    env.ba = BreakoutAnalysis.from_env(env=env)
    _ = env.ba.run_from_env()

    tables = env.ba.get_display_tables()
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.ba.paramater_display_infomation.items()]

    insights_dfs = [env.ba.df_notes, env.ba.breakout_facts, env.ba.subject_facts]
    followups = env.ba.get_suggestions()

    viz, insights, final_prompt, export_data = render_layout(tables,
                                                            env.ba.title,
                                                            env.ba.subtitle,
                                                            insights_dfs,
                                                            env.ba.warning_message,
                                                            env.ba.footnotes,
                                                            parameters.arguments.max_prompt,
                                                            parameters.arguments.insight_prompt)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=insights,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[SuggestedQuestion(label=f.get("label"), question=f.get("question")) for f in followups if f.get("label")],
        export_data=[ExportData(name=name, data=df) for name, df in export_data.items()]
    )

def find_footnote(footnotes, df):
    footnotes = footnotes or {}
    dim_note = None
    for col in df.columns:
        if col in footnotes:
            dim_note = footnotes.get(col)
            break
    return dim_note

def render_layout(tables, title, subtitle, insights_dfs, warnings, footnotes, max_prompt, insight_prompt):
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
        dim_note = find_footnote(footnotes, table)
        template_vars = {
            'dfs': [table],
            "height": height,
            "title": title,
            "subtitle": subtitle,
            "warnings": warnings,
            "dim_note": dim_note
        }
        rendered = template.render(**template_vars)
        viz_list.append(SkillVisualization(title=name, layout=rendered))
    return viz_list, insights, max_response_prompt, export_data


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
    {% if dim_note %}
        ,{
            "name": "footer",
            "type": "Header",
            "row": {{ns.counter + chart_start}},
            "column": 1,
            "width": 120,
            "height": 2,
            "style": {
                "textAlign": "left",
                "verticalAlign": "middle",
                "fontSize": "14px",
                "color": "#333",
                "fontFamily": "Arial, sans-serif",
                "fontStyle": "italic",
                "fontWeight": "normal"
            },
            "text": "*{{dim_note}}"
        }
    {% endif %}
]
}
"""

if __name__ == '__main__':
    skill_input: SkillInput = simple_breakout.create_input(arguments={'metrics': ["sales", "volume"], 'breakouts': ["brand", "manufacturer"], 'periods': ["2022"], 'growth_type': "Y/Y", 'other_filters': []})
    out = simple_breakout(skill_input)
    preview_skill(simple_breakout, out)