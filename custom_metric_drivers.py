from __future__ import annotations
from types import SimpleNamespace

import pandas as pd
import numpy as np
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout
from answer_rocket import AnswerRocketClient

import jinja2
import logging
import json

logger = logging.getLogger(__name__)

@skill(
    name="Custom Metric Drivers Analysis",
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
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods."
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
            default_value=json.dumps({
                "type": "table",
                "title": "{{ headline }}",
                "subtitle": "{{ sub_headline }}",
                "data": "{{ table_data }}",
                "columns": "{{ columns }}",
                "styling": {
                    "variance_column": {
                        "conditional_formatting": {
                            "positive": {"color": "green"},
                            "negative": {"color": "red"}
                        }
                    }
                }
            })
        )
    ]
)
def custom_metric_drivers(parameters: SkillInput) -> SkillOutput:
    """
    Custom Metric Drivers Analysis skill
    
    Compares actual metrics against comparison metrics (plan, target, budget) and provides
    variance analysis with performance indicators.
    """
    try:
        # Initialize AnswerRocket client
        arc = AnswerRocketClient()
        
        if not arc.can_connect():
            logger.error("Failed to connect to AnswerRocket")
            return SkillOutput(
                final_prompt="Unable to connect to AnswerRocket. Please check your connection and credentials.",
                narrative="Connection failed",
                visualizations=[],
                export_data=[]
            )
        
        # Extract parameters
        metric = parameters.arguments.metric
        comparison_metric = parameters.arguments.comparison_metric
        periods = getattr(parameters.arguments, 'periods', [])
        breakouts = getattr(parameters.arguments, 'breakouts', [])
        other_filters = getattr(parameters.arguments, 'other_filters', [])
        limit_n = getattr(parameters.arguments, 'limit_n', 10)
        variance_threshold = getattr(parameters.arguments, 'variance_threshold', 10)
        
        # Build query for AnswerRocket
        query_parts = [f"show me {metric} and {comparison_metric}"]
        
        # Add breakouts
        if breakouts:
            breakout_str = ", ".join(breakouts)
            query_parts.append(f"by {breakout_str}")
        
        # Add time periods
        if periods:
            period_str = ", ".join(periods)
            query_parts.append(f"for {period_str}")
        
        # Add filters
        if other_filters:
            for filter_item in other_filters:
                if isinstance(filter_item, dict):
                    dim = filter_item.get('dim', '')
                    op = filter_item.get('op', '=')
                    val = filter_item.get('val', '')
                    query_parts.append(f"where {dim} {op} {val}")
        
        query = " ".join(query_parts)
        logger.info(f"Executing query: {query}")
        
        # Execute query
        result = arc.ask(query)
        
        # Extract data
        if hasattr(result, 'data') and result.data is not None:
            df = pd.DataFrame(result.data)
        else:
            logger.warning("No data returned from AnswerRocket")
            return SkillOutput(
                final_prompt=f"No data found for {metric} vs {comparison_metric}. Please check your parameters.",
                narrative="No data available",
                visualizations=[],
                export_data=[]
            )
        
        if df.empty:
            return SkillOutput(
                final_prompt=f"No data found for {metric} vs {comparison_metric}. Please check your parameters.",
                narrative="No data available",
                visualizations=[],
                export_data=[]
            )
        
        # Data processing and analysis
        df = process_comparison_data(df, metric, comparison_metric, breakouts, variance_threshold, limit_n)
        
        # Generate insights
        insights_data = generate_insights(df, metric, comparison_metric, variance_threshold)
        
        # Create parameter display descriptions
        param_info = [
            ParameterDisplayDescription(key="Primary Metric", value=metric),
            ParameterDisplayDescription(key="Comparison Metric", value=comparison_metric),
            ParameterDisplayDescription(key="Variance Threshold", value=f"{variance_threshold}%"),
            ParameterDisplayDescription(key="Breakout Dimensions", value=", ".join(breakouts) if breakouts else "None"),
            ParameterDisplayDescription(key="Time Periods", value=", ".join(periods) if periods else "All available")
        ]
        
        # Render layout and create visualizations
        viz, insights, final_prompt, export_data = render_comparison_layout(
            df,
            f"{metric.title()} vs {comparison_metric.title()}",
            f"Performance Analysis with {variance_threshold}% Variance Threshold",
            insights_data,
            [],  # warnings
            parameters.arguments.max_prompt,
            parameters.arguments.insight_prompt,
            parameters.arguments.table_viz_layout
        )
        
        return SkillOutput(
            final_prompt=final_prompt,
            narrative=insights,
            visualizations=viz,
            parameter_display_descriptions=param_info,
            followup_questions=[
                f"What are the top performing {breakouts[0] if breakouts else 'segments'} vs {comparison_metric}?",
                f"Show me the variance trends for {metric} vs {comparison_metric}",
                f"Which factors are driving the {metric} performance gaps?"
            ],
            export_data=[ExportData(name=name, data=df_data) for name, df_data in export_data.items()]
        )
    
    except Exception as e:
        logger.error(f"Error in custom_metric_drivers: {str(e)}")
        return SkillOutput(
            final_prompt=f"An error occurred while analyzing {metric} vs {comparison_metric}: {str(e)}",
            narrative="Error occurred during analysis",
            visualizations=[],
            export_data=[]
        )

def process_comparison_data(df, metric, comparison_metric, breakouts, variance_threshold, limit_n):
    """Process the data to calculate variances and performance indicators"""
    
    # Ensure numeric columns
    df[metric] = pd.to_numeric(df[metric], errors='coerce')
    df[comparison_metric] = pd.to_numeric(df[comparison_metric], errors='coerce')
    
    # Calculate variance metrics
    df['variance'] = df[metric] - df[comparison_metric]
    df['variance_pct'] = ((df[metric] - df[comparison_metric]) / df[comparison_metric] * 100).round(2)
    df['performance_ratio'] = (df[metric] / df[comparison_metric]).round(3)
    
    # Performance indicators
    df['performance_flag'] = np.where(
        df['variance_pct'] > variance_threshold, 'Over Performance',
        np.where(df['variance_pct'] < -variance_threshold, 'Under Performance', 'On Track')
    )
    
    # Achievement percentage
    df['achievement_pct'] = (df[metric] / df[comparison_metric] * 100).round(1)
    
    # Sort by variance to get top performers and underperformers
    df = df.sort_values('variance_pct', ascending=False)
    
    # Limit results if specified
    if limit_n and len(df) > limit_n:
        df = df.head(limit_n)
    
    # Create summary statistics
    df['abs_variance'] = df['variance'].abs()
    
    return df

def generate_insights(df, metric, comparison_metric, variance_threshold):
    """Generate insights about the performance comparison"""
    
    insights = {}
    
    # Overall performance summary
    total_actual = df[metric].sum()
    total_plan = df[comparison_metric].sum()
    overall_variance = total_actual - total_plan
    overall_variance_pct = (overall_variance / total_plan * 100) if total_plan != 0 else 0
    
    insights['overall_performance'] = {
        'total_actual': total_actual,
        'total_plan': total_plan,
        'overall_variance': overall_variance,
        'overall_variance_pct': round(overall_variance_pct, 2)
    }
    
    # Performance flags summary
    performance_summary = df['performance_flag'].value_counts().to_dict()
    insights['performance_summary'] = performance_summary
    
    # Best and worst performers
    if len(df) > 0:
        best_performer = df.iloc[0]
        worst_performer = df.iloc[-1]
        
        insights['best_performer'] = {
            'name': best_performer.get('name', 'Unknown'),
            'variance_pct': best_performer['variance_pct'],
            'achievement_pct': best_performer['achievement_pct']
        }
        
        insights['worst_performer'] = {
            'name': worst_performer.get('name', 'Unknown'),
            'variance_pct': worst_performer['variance_pct'],
            'achievement_pct': worst_performer['achievement_pct']
        }
    
    # Significant variances
    significant_variances = df[df['variance_pct'].abs() > variance_threshold]
    insights['significant_variances'] = len(significant_variances)
    
    return insights

def render_comparison_layout(df, title, subtitle, insights_data, warnings, max_prompt, insight_prompt, viz_layout):
    """Render the visualization layout for comparison analysis"""
    
    # Prepare facts for prompts
    facts = [insights_data]
    
    insight_template = jinja2.Template(insight_prompt).render(facts=facts)
    max_response_prompt = jinja2.Template(max_prompt).render(facts=facts)
    
    # Generate insights using AR Utils (if available)
    try:
        from ar_analytics import ArUtils
        ar_utils = ArUtils()
        insights = ar_utils.get_llm_response(insight_template)
    except ImportError:
        insights = "Analysis completed. Review the data for performance insights."
    
    # Prepare visualization data
    viz_list = []
    export_data = {"Performance Analysis": df}
    
    # General variables for layout
    general_vars = {
        "headline": title,
        "sub_headline": subtitle,
        "hide_growth_warning": len(warnings) == 0,
        "exec_summary": insights,
        "warning": warnings
    }
    
    # Prepare table data
    table_data = df.to_dict('records')
    columns = [
        {"field": col, "title": col.replace('_', ' ').title()} 
        for col in df.columns
    ]
    
    table_vars = {
        "table_data": table_data,
        "columns": columns,
        "total_rows": len(df)
    }
    
    # Wire the layout
    try:
        rendered = wire_layout(json.loads(viz_layout), {**general_vars, **table_vars})
        viz_list.append(SkillVisualization(title="Performance Analysis", layout=rendered))
    except Exception as e:
        logger.error(f"Error rendering layout: {e}")
        # Fallback simple table
        simple_layout = {
            "type": "table",
            "title": title,
            "data": table_data,
            "columns": columns
        }
        viz_list.append(SkillVisualization(title="Performance Analysis", layout=simple_layout))
    
    return viz_list, insights, max_response_prompt, export_data

if __name__ == '__main__':
    # Test the skill
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