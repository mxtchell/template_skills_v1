from __future__ import annotations
from types import SimpleNamespace

import pandas as pd
import numpy as np
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout

from answer_rocket import AnswerRocketClient
from ar_analytics.defaults import default_table_layout, get_table_layout_vars
from ar_analytics import ArUtils

import jinja2
import logging
import json

logger = logging.getLogger(__name__)

@skill(
    name="Driver Analysis",
    description="Analyzes performance drivers by comparing actual metrics against comparison metrics (like plan, target, or other business metrics). Shows variance, percentage differences, and performance indicators across different dimensions.",
    capabilities="Can compare any metric against another metric (e.g., sales vs sales_plan, volume vs volume_target). Provides variance analysis, percentage differences, and performance breakdowns by dimensions. Supports filtering and custom analysis periods.",
    limitations="Requires both actual and comparison metrics to exist in the dataset. Cannot perform time-based growth analysis (use standard metric drivers for Y/Y or P/P analysis).",
    example_questions=[
        "Show me sales vs sales plan by region",
        "How did volume perform against target by product line?",
        "What's the variance between actual revenue and budget by quarter?"
    ],
    parameter_guidance="Specify the main metric to analyze, the comparison metric (plan/target/budget), and optional breakout dimensions for detailed analysis.",
    parameters=[
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', etc."
        ),
        SkillParameter(
            name="metric",
            is_multi=False,
            constrained_to="metrics",
            description="The primary metric to analyze (e.g., sales, volume, revenue)",
            required=True
        ),
        SkillParameter(
            name="comparison_metric",
            is_multi=False,
            constrained_to="metrics", 
            description="The metric to compare against (e.g., sales_plan, volume_target, revenue_budget)",
            required=True
        ),
        SkillParameter(
            name="limit_n",
            description="Limit the number of values by this number",
            default_value=10
        ),
        SkillParameter(
            name="breakouts",
            is_multi=True,
            constrained_to="dimensions",
            description="Breakout dimension(s) for analysis (e.g., region, product, channel)"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters",
            description="Additional filters to apply to the analysis"
        ),
        SkillParameter(
            name="variance_threshold",
            description="Threshold percentage for highlighting significant variances (default: 10%)",
            default_value=10
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value="Based on the analysis facts: {facts}, provide a comprehensive summary of the metric performance vs comparison metric, highlighting key insights, significant variances, and actionable recommendations."
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt", 
            description="Prompt being used for detailed insights.",
            default_value="Analyze the performance data and provide detailed insights about: 1) Overall performance vs target/plan, 2) Key drivers of variance, 3) Best and worst performing segments, 4) Recommendations for improvement. Facts: {facts}"
        ),
        SkillParameter(
            name="table_viz_layout",
            parameter_type="visualization",
            description="Table Viz Layout",
            default_value=default_table_layout
        )
    ]
)
def custom_metric_drivers(parameters: SkillInput):
    """
    Driver Analysis skill
    
    Compares actual metrics against comparison metrics in the same period and provides variance analysis.
    """
    print("DEBUG: Starting Driver Analysis skill")
    print(f"DEBUG: Raw parameters received: {parameters.arguments}")
    
    param_dict = {
        "periods": [], 
        "metric": "", 
        "comparison_metric": "",
        "limit_n": 10, 
        "breakouts": [], 
        "other_filters": [], 
        "variance_threshold": 10
    }
    
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    print(f"DEBUG: Processed parameters: {param_dict}")

    env = SimpleNamespace(**param_dict)
    
    # Initialize AnswerRocket client
    arc = AnswerRocketClient()
    
    print("DEBUG: Testing AnswerRocket connection...")
    if not arc.can_connect():
        print("DEBUG: AnswerRocket connection failed")
        return SkillOutput(
            final_prompt="Unable to connect to AnswerRocket. Please check your connection and credentials.",
            narrative="Connection failed",
            visualizations=[],
            export_data=[]
        )
    
    print("DEBUG: AnswerRocket connection successful")
    
    # Build query for AnswerRocket - comparing different metrics in same period
    query_parts = [f"show me {env.metric} and {env.comparison_metric}"]
    
    # Add breakouts
    if env.breakouts:
        breakout_str = ", ".join(env.breakouts)
        query_parts.append(f"by {breakout_str}")
        print(f"DEBUG: Added breakouts: {breakout_str}")
    
    # Add time periods
    if env.periods:
        period_str = ", ".join(env.periods)
        query_parts.append(f"for {period_str}")
        print(f"DEBUG: Added periods: {period_str}")
    
    # Add filters
    if env.other_filters:
        print(f"DEBUG: Processing {len(env.other_filters)} filters")
        for i, filter_item in enumerate(env.other_filters):
            if isinstance(filter_item, dict):
                dim = filter_item.get('dim', '')
                op = filter_item.get('op', '=')
                val = filter_item.get('val', '')
                query_parts.append(f"where {dim} {op} {val}")
                print(f"DEBUG: Filter {i}: {dim} {op} {val}")
    
    query = " ".join(query_parts)
    print(f"DEBUG: Final query: {query}")
    
    # Execute query
    print("DEBUG: Executing AnswerRocket query...")
    result = arc.ask(query)
    
    # Extract data
    if hasattr(result, 'data') and result.data is not None:
        df = pd.DataFrame(result.data)
        print(f"DEBUG: Created DataFrame with shape: {df.shape}")
        print(f"DEBUG: DataFrame columns: {df.columns.tolist()}")
    else:
        print("DEBUG: No data returned from AnswerRocket")
        return SkillOutput(
            final_prompt=f"No data found for {env.metric} vs {env.comparison_metric}. Please check your parameters.",
            narrative="No data available",
            visualizations=[],
            export_data=[]
        )
    
    if df.empty:
        print("DEBUG: DataFrame is empty")
        return SkillOutput(
            final_prompt=f"No data found for {env.metric} vs {env.comparison_metric}. Please check your parameters.",
            narrative="No data available", 
            visualizations=[],
            export_data=[]
        )
    
    # Process the data to add variance calculations - this is the custom logic
    print(f"DEBUG: Processing data for variance analysis...")
    
    # Check if required columns exist
    if env.metric not in df.columns:
        print(f"DEBUG: Metric '{env.metric}' not found in columns: {df.columns.tolist()}")
        return SkillOutput(
            final_prompt=f"Metric '{env.metric}' not found in the data. Available columns: {', '.join(df.columns)}",
            narrative="Missing metric data",
            visualizations=[],
            export_data=[]
        )
    
    if env.comparison_metric not in df.columns:
        print(f"DEBUG: Comparison metric '{env.comparison_metric}' not found in columns: {df.columns.tolist()}")
        return SkillOutput(
            final_prompt=f"Comparison metric '{env.comparison_metric}' not found in the data. Available columns: {', '.join(df.columns)}",
            narrative="Missing comparison metric data",
            visualizations=[],
            export_data=[]
        )
    
    # Ensure numeric columns
    df[env.metric] = pd.to_numeric(df[env.metric], errors='coerce')
    df[env.comparison_metric] = pd.to_numeric(df[env.comparison_metric], errors='coerce')
    
    print(f"DEBUG: {env.metric} null values: {df[env.metric].isna().sum()}")
    print(f"DEBUG: {env.comparison_metric} null values: {df[env.comparison_metric].isna().sum()}")
    
    # Calculate variance metrics - comparing same period metrics
    df['variance'] = df[env.metric] - df[env.comparison_metric]
    df['variance_pct'] = ((df[env.metric] - df[env.comparison_metric]) / df[env.comparison_metric] * 100).round(2)
    df['achievement_pct'] = (df[env.metric] / df[env.comparison_metric] * 100).round(1)
    
    print(f"DEBUG: Variance stats - min: {df['variance'].min():.2f}, max: {df['variance'].max():.2f}")
    print(f"DEBUG: Variance pct stats - min: {df['variance_pct'].min():.2f}%, max: {df['variance_pct'].max():.2f}%")
    
    # Performance indicators
    df['performance_flag'] = np.where(
        df['variance_pct'] > env.variance_threshold, 'Over Performance',
        np.where(df['variance_pct'] < -env.variance_threshold, 'Under Performance', 'On Track')
    )
    
    print(f"DEBUG: Performance flag distribution:")
    print(df['performance_flag'].value_counts())
    
    # Sort by variance to get top performers and underperformers
    df = df.sort_values('variance_pct', ascending=False)
    
    # Limit results if specified
    if env.limit_n and len(df) > env.limit_n:
        print(f"DEBUG: Limiting results to top {env.limit_n} rows")
        df = df.head(env.limit_n)
    
    print(f"DEBUG: Final processed DataFrame shape: {df.shape}")
    
    # Create tables dictionary following metric_drivers.py pattern
    tables = {"Variance Analysis": df}
    
    # Create parameter display descriptions
    param_info = [
        ParameterDisplayDescription(key="Primary Metric", value=env.metric),
        ParameterDisplayDescription(key="Comparison Metric", value=env.comparison_metric),
        ParameterDisplayDescription(key="Variance Threshold", value=f"{env.variance_threshold}%"),
        ParameterDisplayDescription(key="Breakout Dimensions", value=", ".join(env.breakouts) if env.breakouts else "None")
    ]
    
    # Generate insights facts - summarize variance analysis
    total_actual = df[env.metric].sum()
    total_plan = df[env.comparison_metric].sum()
    overall_variance_pct = ((total_actual - total_plan) / total_plan * 100) if total_plan != 0 else 0
    
    insights_dfs = [pd.DataFrame([{
        'metric': env.metric,
        'comparison_metric': env.comparison_metric,
        'total_actual': total_actual,
        'total_plan': total_plan,
        'overall_variance_pct': round(overall_variance_pct, 2),
        'performance_summary': df['performance_flag'].value_counts().to_dict(),
        'significant_variances': len(df[df['variance_pct'].abs() > env.variance_threshold])
    }])]
    
    print(f"DEBUG: Overall variance: {overall_variance_pct:.2f}%")
    
    # Render layout following the same pattern as metric_drivers.py
    viz, insights, final_prompt, export_data = render_layout(
        tables,
        f"{env.metric.title()} vs {env.comparison_metric.title()}",
        f"Variance Analysis with {env.variance_threshold}% Threshold",
        insights_dfs,
        [],  # warnings
        parameters.arguments.max_prompt,
        parameters.arguments.insight_prompt,
        parameters.arguments.table_viz_layout
    )
    
    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[
            f"What are the top performing {env.breakouts[0] if env.breakouts else 'segments'} vs {env.comparison_metric}?",
            f"Show me the variance trends for {env.metric} vs {env.comparison_metric}",
            f"Which factors are driving the {env.metric} performance gaps?"
        ],
        export_data=[ExportData(name=name, data=df_data) for name, df_data in export_data.items()]
    )

def render_layout(tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt, viz_layout):
    """Render the visualization layout following the exact same pattern as metric_drivers.py"""
    
    print(f"DEBUG: render_layout called with {len(tables)} tables")
    
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})

    print(f"DEBUG: Generated insight template length: {len(insight_template)}")
    print(f"DEBUG: Generated max response prompt length: {len(max_response_prompt)}")

    # Generate insights using ArUtils like other skills
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)
    
    print(f"DEBUG: LLM insights generated, length: {len(insights)}")
    
    viz_list = []
    export_data = {}

    general_vars = {
        "headline": title if title else "Total",
        "sub_headline": subtitle if subtitle else "Variance Analysis", 
        "hide_growth_warning": False if warnings else True,
        "exec_summary": insights if insights else "No Insights.",
        "warning": warnings
    }

    print(f"DEBUG: General vars prepared")

    for name, table in tables.items():
        export_data[name] = table
        hide_footer = True
        table_vars = get_table_layout_vars(table)
        table_vars["hide_footer"] = hide_footer
        rendered = wire_layout(json.loads(viz_layout), {**general_vars, **table_vars})
        viz_list.append(SkillVisualization(title=name, layout=rendered))
        print(f"DEBUG: Created visualization for {name}")

    print(f"DEBUG: render_layout returning {len(viz_list)} visualizations")
    return viz_list, insights, max_response_prompt, export_data

if __name__ == '__main__':
    skill_input: SkillInput = custom_metric_drivers.create_input(
        arguments={
            "metric": "sales",
            "comparison_metric": "sales_plan",
            "breakouts": ["region"],
            "periods": ["2023"],
            "variance_threshold": 10
        }
    )
    out = custom_metric_drivers(skill_input)
    preview_skill(custom_metric_drivers, out)