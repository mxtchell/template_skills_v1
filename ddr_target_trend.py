from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import jinja2
from ar_analytics import AdvanceTrend, TrendTemplateParameterSetup, ArUtils
from ar_analytics.defaults import trend_analysis_config, default_trend_chart_layout, default_table_layout, \
    get_table_layout_vars, default_ppt_trend_chart_layout, default_ppt_table_layout
from skill_framework import SkillVisualization, skill, SkillParameter, SkillInput, SkillOutput, \
    ParameterDisplayDescription
from skill_framework.layouts import wire_layout
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData

RUNNING_LOCALLY = False

logger = logging.getLogger(__name__)

print("DEBUG: Initializing DDR vs Target Trend skill")

@skill(
    name="DDR vs Target Trend",
    llm_name="DDR_vs_Target_Trend_Analysis",
    description="Analyzes DDR1 or DDR2 detection rates against targets over time periods. ONLY use when user mentions DDR1, DDR2, plan, or target. Do not use for other metrics.",
    capabilities="Compares actual DDR performance vs targets, identifies over/under-performing periods, shows variance trends. Restricted to DDR metrics only.",
    limitations="Only works with DDR1/DDR2 metrics and their targets. For all other metrics, use the standard trend analysis skill.",
    example_questions="How did my branch perform in DDR2 last week? Show me DDR1 vs target for Q2 2019. DDR2 performance vs plan by region last month. DDR1 vs target trending over the last 6 months.",
    parameter_guidance="Select DDR1 or DDR2 for analysis. Specify time periods and optional breakout dimensions. Focus on performance vs target comparison.",
    parameters=[
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods. If given a range, for example 'last 3 quarters, 'between q3 2022 to q4 2023' etc, enumerate the range into a list of valid dates. Don't include natural language words or phrases, only valid dates like 'q3 2023', '2022', 'mar 2020', 'ytd sep 2021', 'mat q4 2021', 'ytd q1 2022', 'ytd 2021', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>' etc."
        ),
        SkillParameter(
            name="ddr_pair",
            constrained_values=["DDR1", "DDR2"],
            description="Select which DDR metric pair to analyze: DDR1 (ddr1 vs target_ddr1) or DDR2 (ddr2 vs target_ddr2)",
            required=True
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
            description="breakout dimension(s) for analysis. DISABLED for DDR vs Target analysis - always empty."
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
            constrained_values=["None"],
            description="Growth type set to None to avoid automatic target metric construction",
            default_value="None"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value="Analyze DDR vs target performance in 100-150 words: {{facts}}. If analyzing a specific branch, use 'your branch' language. Focus on: 1) How DDR performed against target (above/below/aligned), 2) Key trends over time, 3) Notable periods or changes. Avoid mathematical sums - DDR is a percentage rate."
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value="Provide a 100-150 word analysis of DDR vs target: {{facts}}. When analyzing a specific branch, use personalized language like 'your branch'. Include: 1) Overall performance vs target, 2) Trend direction (improving/declining), 3) Key periods of interest, 4) Brief recommendations. Remember DDR is a percentage rate - avoid summing percentages."
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
            name="chart_ppt_layout",
            parameter_type="visualization",
            description="chart slide Viz Layout",
            default_value=default_ppt_trend_chart_layout
        ),
        SkillParameter(
            name="table_ppt_export_viz_layout",
            parameter_type="visualization",
            description="table slide Viz Layout",
            default_value=default_ppt_table_layout
        )
    ]
)
def ddr_target_trend(parameters: SkillInput):
    print("DEBUG: Starting DDR vs Target Trend skill execution")
    print(f"DEBUG: Skill received following parameters: {parameters.arguments}")
    
    # DEBUG: Inspect default chart layout for single axis modification
    print(f"DEBUG: Default chart layout preview: {parameters.arguments.chart_viz_layout[:500]}...")
    
    # Initialize parameter dictionary with DDR-specific defaults
    param_dict = {
        "periods": [], 
        "metrics": None,  # Will be set based on ddr_pair selection
        "limit_n": 10, 
        "breakouts": [],  # Always empty for DDR vs Target analysis
        "growth_type": "None", 
        "other_filters": [], 
        "time_granularity": None,
        "ddr_pair": None
    }

    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            if key == "breakouts":
                # Force breakouts to always be empty for DDR vs Target analysis
                param_dict[key] = []
                print(f"DEBUG: Forced {key} = [] (disabled for DDR vs Target)")
            else:
                param_dict[key] = getattr(parameters.arguments, key)
                print(f"DEBUG: Set {key} = {param_dict[key]}")

    # Map DDR pair selection to actual metrics
    ddr_pair = param_dict.get("ddr_pair")
    print(f"DEBUG: Selected DDR pair: {ddr_pair}")
    
    if ddr_pair == "DDR1":
        param_dict["metrics"] = ["ddr1", "target_ddr1"]
        print("DEBUG: Mapped DDR1 to metrics: ['ddr1', 'target_ddr1']")
    elif ddr_pair == "DDR2":
        param_dict["metrics"] = ["ddr2", "target_ddr2"]
        print("DEBUG: Mapped DDR2 to metrics: ['ddr2', 'target_ddr2']")
    else:
        print(f"DEBUG: ERROR - Invalid or missing DDR pair selection: {ddr_pair}")
        raise ValueError(f"Invalid DDR pair selection: {ddr_pair}. Must be 'DDR1' or 'DDR2'")

    print(f"DEBUG: Final param_dict: {param_dict}")

    env = SimpleNamespace(**param_dict)
    print(f"DEBUG: Created environment namespace with metrics: {env.metrics}")
    
    TrendTemplateParameterSetup(env=env)
    print("DEBUG: Completed TrendTemplateParameterSetup")
    
    env.trend = AdvanceTrend.from_env(env=env)
    print(f"DEBUG: Created AdvanceTrend object: {type(env.trend)}")
    
    df = env.trend.run_from_env()
    print(f"DEBUG: Executed trend analysis, resulting dataframe shape: {df.shape if df is not None else 'None'}")
    
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.trend.paramater_display_infomation.items()]
    print(f"DEBUG: Created {len(param_info)} parameter display descriptions")
    
    tables = [env.trend.display_dfs.get("Metrics Table")]
    print(f"DEBUG: Retrieved metrics table: {tables[0].shape if tables[0] is not None else 'None'}")

    insights_dfs = [env.trend.df_notes, env.trend.facts, env.trend.top_facts, env.trend.bottom_facts]
    print(f"DEBUG: Retrieved {len([df for df in insights_dfs if df is not None])} insight dataframes")

    charts = env.trend.get_dynamic_layout_chart_vars()
    print(f"DEBUG: Retrieved chart variables for {len(charts)} charts: {list(charts.keys())}")
    
    # DEBUG: Inspect chart configuration for single axis solution
    for chart_name, chart_vars in charts.items():
        print(f"DEBUG: Chart '{chart_name}' variables:")
        for key, value in chart_vars.items():
            if 'axis' in key.lower() or 'series' in key.lower():
                print(f"DEBUG:   {key}: {str(value)[:200]}...")
        print(f"DEBUG: Chart '{chart_name}' has {len(chart_vars)} total variables")
    
    # CRITICAL FIX: Modify chart variables to force single Y-axis BEFORE layout rendering
    charts = force_single_axis_chart_vars(charts)

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
                                               parameters.arguments.chart_ppt_layout,
                                               parameters.arguments.table_ppt_export_viz_layout)

    print(f"DEBUG: Completed layout rendering with {len(viz)} visualizations and {len(slides)} slides")

    display_charts = env.trend.display_charts
    print(f"DEBUG: Retrieved display charts: {list(display_charts.keys()) if display_charts else 'None'}")

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        ppt_slides=slides,
        parameter_display_descriptions=param_info,
        followup_questions=[],
        export_data=[ExportData(name="Metrics Table", data=tables[0]),
                     *[ExportData(name=chart, data=display_charts[chart].get("df")) for chart in display_charts.keys()]]
    )

def force_single_axis_chart_vars(charts):
    """
    CRITICAL FIX: Modify chart variables to force single Y-axis.
    This intercepts AdvanceTrend's dual-axis configuration before layout rendering.
    """
    print("DEBUG: CRITICAL FIX - Forcing single Y-axis in chart variables")
    
    modified_charts = {}
    for chart_name, chart_vars in charts.items():
        print(f"DEBUG: Processing chart variables for: {chart_name}")
        modified_vars = chart_vars.copy()
        
        # Fix the Y-axis configuration - this is the key fix
        if 'absolute_y_axis' in modified_vars:
            original_y_axis = modified_vars['absolute_y_axis']
            print(f"DEBUG: Original Y-axis config: {original_y_axis}")
            
            if isinstance(original_y_axis, list) and len(original_y_axis) > 1:
                # FORCE SINGLE AXIS: Use only the first axis, combine titles
                first_axis = original_y_axis[0].copy()
                second_axis = original_y_axis[1]
                
                # Combine titles to show both metrics
                combined_title = f"{first_axis.get('title', '')} & {second_axis.get('title', '')}"
                first_axis['title'] = combined_title
                first_axis['opposite'] = False  # Force to left side only
                
                # Replace dual-axis with single axis
                modified_vars['absolute_y_axis'] = [first_axis]  # Single axis in array
                print(f"DEBUG: FIXED Y-axis to single axis: {modified_vars['absolute_y_axis']}")
            
        # Ensure all series use the same Y-axis (axis 0)
        if 'absolute_series' in modified_vars:
            series_data = modified_vars['absolute_series']
            if isinstance(series_data, list):
                for i, series in enumerate(series_data):
                    if isinstance(series, dict):
                        series['yAxis'] = 0  # Force all series to use axis 0
                        print(f"DEBUG: Set series {i} '{series.get('name', '')}' to use yAxis 0")
        
        modified_charts[chart_name] = modified_vars
        print(f"DEBUG: Completed single-axis fix for chart: {chart_name}")
    
    print("DEBUG: CRITICAL FIX COMPLETE - All charts now use single Y-axis")
    return modified_charts

def create_single_axis_chart_layout(default_layout):
    """
    Modify the chart layout to force all metrics onto a single Y-axis.
    This overrides AdvanceTrend's default dual-axis behavior.
    """
    print("DEBUG: Creating single-axis chart layout override")
    
    try:
        import json
        layout_data = json.loads(default_layout)
        print(f"DEBUG: Parsed chart layout with {len(str(layout_data))} characters")
        
        # Find and modify Highcharts configuration
        def modify_highcharts_config(obj, path=""):
            if isinstance(obj, dict):
                # Look for Highcharts chart configuration
                if 'type' in obj and obj.get('type') == 'HighchartsChart':
                    print(f"DEBUG: Found HighchartsChart at {path}")
                    if 'options' in obj:
                        options = obj['options']
                        print(f"DEBUG: Modifying Highcharts options")
                        
                        # Force single Y-axis configuration
                        if 'yAxis' in options:
                            # If yAxis is an array (multiple axes), force to single axis
                            if isinstance(options['yAxis'], list):
                                print("DEBUG: Converting multiple Y-axes to single axis")
                                options['yAxis'] = options['yAxis'][0]  # Use first axis only
                            
                            # Ensure single axis configuration
                            if isinstance(options['yAxis'], dict):
                                options['yAxis']['opposite'] = False  # Force to left side
                                print("DEBUG: Set yAxis opposite = False")
                        
                        # Ensure all series use the same Y-axis
                        if 'series' in options and isinstance(options['series'], list):
                            for i, series in enumerate(options['series']):
                                if isinstance(series, dict):
                                    series['yAxis'] = 0  # Force all series to use axis 0
                                    print(f"DEBUG: Set series {i} to use yAxis 0")
                        
                        print("DEBUG: Single-axis modification complete")
                
                # Recursively process nested objects
                for key, value in obj.items():
                    modify_highcharts_config(value, f"{path}.{key}")
            
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    modify_highcharts_config(item, f"{path}[{i}]")
        
        modify_highcharts_config(layout_data)
        modified_layout = json.dumps(layout_data)
        print(f"DEBUG: Created modified single-axis layout")
        return modified_layout
        
    except Exception as e:
        print(f"DEBUG: Error modifying chart layout: {e}")
        return default_layout

def map_chart_variables(chart_vars, prefix):
    """
    Maps prefixed chart variables to generic variable names expected by the layout.

    Args:
        chart_vars: Dictionary containing all chart variables with prefixes
        prefix: The prefix to extract (e.g., 'absolute_', 'growth_', 'difference_')

    Returns:
        Dictionary with mapped variables using generic names
    """
    print(f"DEBUG: Mapping chart variables with prefix: {prefix}")
    suffixes = ['series', 'x_axis_categories', 'y_axis', 'metric_name', 'meta_df_id']

    mapped_vars = {}

    for suffix in suffixes:
        prefixed_key = f"{prefix}{suffix}"
        if prefixed_key in chart_vars:
            mapped_vars[suffix] = chart_vars[prefixed_key]
            print(f"DEBUG: Mapped {prefixed_key} to {suffix}")

    if 'footer' in chart_vars:
        mapped_vars['footer'] = chart_vars['footer']
    if 'hide_footer' in chart_vars:
        mapped_vars['hide_footer'] = chart_vars['hide_footer']

    print(f"DEBUG: Final mapped variables: {list(mapped_vars.keys())}")
    return mapped_vars

def render_layout(charts, tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt, table_viz_layout, chart_viz_layout, chart_ppt_layout, table_ppt_export_viz_layout):
    print("DEBUG: Starting layout rendering")
    print(f"DEBUG: Charts to render: {list(charts.keys()) if charts else 'None'}")
    print(f"DEBUG: Title: {title}, Subtitle: {subtitle}")
    
    facts = []
    for i, i_df in enumerate(insights_dfs):
        if i_df is not None:
            df_facts = i_df.to_dict(orient='records')
            facts.append(df_facts)
            print(f"DEBUG: Added {len(df_facts)} facts from insight dataframe {i}")
        else:
            facts.append([])
            print(f"DEBUG: Insight dataframe {i} is None")

    print(f"DEBUG: Total fact groups: {len(facts)}")

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})
    print("DEBUG: Rendered insight and max response prompts")

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)
    print(f"DEBUG: Generated insights: {insights[:100] if insights else 'None'}...")

    tab_vars = {"headline": title if title else "DDR vs Target Analysis",
                "sub_headline": subtitle or "DDR Performance vs Target Trend Analysis",
                "hide_growth_warning": False if warnings else True,
                "exec_summary": insights if insights else "No DDR vs Target insights available.",
                "warning": warnings}
    print(f"DEBUG: Created table variables: {list(tab_vars.keys())}")

    viz = []
    slides = []
    
    for name, chart_vars in charts.items():
        print(f"DEBUG: Processing chart: {name}")
        chart_vars["footer"] = f"*{chart_vars['footer']}" if chart_vars.get('footer') else "DDR vs Target Analysis"
        
        # Use regular layout - chart variables have already been fixed for single axis
        rendered = wire_layout(json.loads(chart_viz_layout), {**tab_vars, **chart_vars})
        viz.append(SkillVisualization(title=name, layout=rendered))
        print(f"DEBUG: Added visualization for chart: {name} with single-axis variables")

        prefixes = ["absolute_", "growth_", "difference_"]

        for prefix in prefixes:
            if (prefix in ["growth_", "difference_"] and
                chart_vars.get("hide_growth_chart", False)):
                print(f"DEBUG: Skipping {prefix} chart due to hide_growth_chart setting")
                continue

            try:
                mapped_vars = map_chart_variables(chart_vars, prefix)
                slide = wire_layout(json.loads(chart_ppt_layout), {**tab_vars, **mapped_vars})
                slides.append(slide)
                print(f"DEBUG: Added PPT slide for {prefix} chart")
            except Exception as e:
                logger.error(f"Error rendering chart ppt slide for prefix '{prefix}' in chart '{name}': {e}")
                print(f"DEBUG: ERROR rendering PPT slide for {prefix}: {e}")

    table_vars = get_table_layout_vars(tables[0])
    table = wire_layout(json.loads(table_viz_layout), {**tab_vars, **table_vars})
    viz.append(SkillVisualization(title="DDR vs Target Metrics Table", layout=table))
    print("DEBUG: Added metrics table visualization")

    if table_ppt_export_viz_layout is not None:
        try: 
            table_slide = wire_layout(json.loads(table_ppt_export_viz_layout), {**tab_vars, **table_vars})
            slides.append(table_slide)
            print("DEBUG: Added table PPT slide")
        except Exception as e:
            logger.error(f"Error rendering table ppt slide: {e}")
            print(f"DEBUG: ERROR rendering table PPT slide: {e}")
    else:
        slides.append(table)
        print("DEBUG: Added table as slide (no specific PPT layout)")

    print(f"DEBUG: Layout rendering complete - {len(viz)} visualizations, {len(slides)} slides")
    return viz, slides, insights, max_response_prompt

if __name__ == '__main__':
    print("DEBUG: Running DDR vs Target Trend skill in test mode")
    skill_input: SkillInput = ddr_target_trend.create_input(arguments={
        'ddr_pair': 'DDR1',
        'periods': ["2019", "2018"],
        'growth_type': "None",
        "other_filters": [{"dim": "branch", "op": "=", "val": ["branch_a"]}]
    })
    print(f"DEBUG: Created test input: {skill_input.arguments}")
    out = ddr_target_trend(skill_input)
    preview_skill(ddr_target_trend, out)
    print("DEBUG: Test execution completed")