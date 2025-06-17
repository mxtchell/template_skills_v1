from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillVisualization, skill, SkillParameter, SkillInput, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout

from ar_analytics import AdvanceTrend, TrendTemplateParameterSetup, ArUtils
from ar_analytics.defaults import trend_analysis_config, default_trend_chart_layout, default_table_layout, default_ppt_table_layout, default_ppt_trend_chart_layout, default_ppt_trend_growth_chart_layout, default_ppt_trend_diff_chart_layout, get_table_layout_vars

import jinja2
import logging
import json

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
        ),
        SkillParameter(
            name="table_viz_layout",
            parameter_type="visualization",
            description="Table Viz Layout",
            default_value=default_table_layout
        ),
        SkillParameter(
            name="chart_viz_layout",
            parameter_type="visualization",
            description="Chart Viz Layout",
            default_value=default_trend_chart_layout
        ),
        SkillParameter(
            name="abs_chart_ppt_layout",
            parameter_type="visualization",
            description="abs chart slide Viz Layout",
            default_value=default_ppt_trend_chart_layout
        ),
        SkillParameter(
            name="growth_chart_ppt_layout",
            parameter_type="visualization",
            description="growth chart slide Viz Layout",
            default_value=default_ppt_trend_growth_chart_layout
        ),
        SkillParameter(
            name="diff_chart_ppt_layout",
            parameter_type="visualization",
            description="diff chart slide Viz Layout",
            default_value=default_ppt_trend_diff_chart_layout
        ),
        SkillParameter(
            name="table_ppt_export_viz_layout",
            parameter_type="visualization",
            description="table slide Viz Layout",
            default_value=default_ppt_table_layout
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
    tables = [env.trend.display_dfs.get("Metrics Table")]

    insights_dfs = [env.trend.df_notes, env.trend.facts, env.trend.top_facts, env.trend.bottom_facts]

    charts = env.trend.get_dynamic_layout_chart_vars()

    viz, slides, insights, final_prompt = render_layout(charts,
                                                tables,
                                                env.trend.title,
                                                env.trend.subtitle,
                                                insights_dfs,
                                                env.trend.warning_message,
                                                parameters.arguments.max_prompt,
                                                parameters.arguments.insight_prompt,
                                                parameters.arguments.table_viz_layout,
                                                parameters.arguments.chart_viz_layout,
                                                parameters.arguments.abs_chart_ppt_layout,
                                                parameters.arguments.growth_chart_ppt_layout,
                                                parameters.arguments.diff_chart_ppt_layout,
                                                parameters.arguments.table_ppt_export_viz_layout)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        ppt_slides=slides,
        parameter_display_descriptions=param_info,
        followup_questions=[],
        export_data=[ExportData(name="Metrics Table", data=tables[0])]
    )

def render_layout(charts, tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt, table_viz_layout, chart_viz_layout, abs_chart_ppt_layout, growth_chart_ppt_layout, diff_chart_ppt_layout, table_ppt_export_viz_layout):
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)

    tab_vars = {"headline": title if title else "Total",
                "sub_headline": subtitle or "Trend Analysis",
                "hide_growth_warning": False if warnings else True,
                "exec_summary": insights if insights else "No Insight.",
                "warning": warnings}

    viz = []
    slides = []
    for name, chart_vars in charts.items():
        chart_vars["footer"] = f"*{chart_vars['footer']}" if chart_vars.get('footer') else "No additional info."
        rendered = wire_layout(json.loads(chart_viz_layout), {**tab_vars, **chart_vars})
        viz.append(SkillVisualization(title=name, layout=rendered))
        try:
            abs_slide = wire_layout(json.loads(abs_chart_ppt_layout), {**tab_vars, **chart_vars})
            slides.append(abs_slide)
            if "hide_growth_chart" not in chart_vars or not chart_vars["hide_growth_chart"]:
                growth_slide = wire_layout(json.loads(growth_chart_ppt_layout), {**tab_vars, **chart_vars})
                slides.append(growth_slide)
                diff_slide = wire_layout(json.loads(diff_chart_ppt_layout), {**tab_vars, **chart_vars})
                slides.append(diff_slide)
        except Exception as e:
            logger.error(f"Error rendering chart ppt slide: {e}")

    table_vars = get_table_layout_vars(tables[0])
    table = wire_layout(json.loads(table_viz_layout), {**tab_vars, **table_vars})
    viz.append(SkillVisualization(title="Metrics Table", layout=table))

    if table_ppt_export_viz_layout is not None:
        try: 
            table_slide = wire_layout(json.loads(table_ppt_export_viz_layout), {**tab_vars, **table_vars})
            slides.append(table_slide)
        except Exception as e:
            logger.error(f"Error rendering table ppt slide: {e}")
    else:
        slides.append(table)

    return viz, slides, insights, max_response_prompt

if __name__ == '__main__':
    # Create a custom slides visualization layout
    custom_slides_layout = default_trend_chart_layout  # You can customize this if needed

    skill_input: SkillInput = trend.create_input(arguments={
        'metrics': ["sales", "volume"],
        'periods': ["2021", "2022"],
        'growth_type': "Y/Y",
        "other_filters": [{"dim": "brand", "op": "=", "val": ["barilla"]}],
        "slides_viz_layout": custom_slides_layout
    })
    out = trend(skill_input)
    preview_skill(trend, out)