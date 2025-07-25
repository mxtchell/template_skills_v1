from __future__ import annotations
from types import SimpleNamespace
from enum import Enum

import pandas as pd
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout

from ar_analytics import ArUtils, AdvanceTrend, TrendTemplateParameterSetup
from ar_analytics.defaults import default_table_layout, get_table_layout_vars, default_trend_chart_layout
from ar_analytics.driver_analysis import DriverAnalysis, DriverAnalysisTemplateParameterSetup
from ar_analytics.helpers.utils import Connector, exit_with_status, NO_LIMIT_N, fmt_sign_num
from ar_analytics.metric_tree import MetricTreeAnalysis
from ar_analytics.breakout_drivers import BreakoutDrivers

import jinja2
import logging
import json
import calendar

logger = logging.getLogger(__name__)

# MAIN SKILL
@skill(
    name="Sixt Plan Drivers",
    llm_name="sixt_plan_driver_analysis",
    description="Analyzes DDR performance drivers by comparing actual metrics against targets. Shows variance, percentage differences, and performance indicators across different dimensions for Sixt car rental data.",
    capabilities="Specialized for DDR1 and DDR2 vs target analysis. Provides variance analysis, percentage differences, and performance breakdowns by dimensions like branch, region, product. Supports vs target comparison for damage detection metrics.",
    limitations="Optimized for DDR metrics with target comparison. For other metrics or time-based growth analysis, use standard metric drivers.",
    example_questions="What's driving DDR1 performance vs target by branch? How did DDR2 perform against plan by region? Show me variance between actual damage detection and target by month.",
    parameter_guidance="Select DDR1 or DDR2 for analysis. Specify time periods and optional breakout dimensions. Focus on performance vs target/plan comparison for damage detection metrics.",
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
            constrained_to="metrics",
            description="The metric to analyze (typically DDR1 or DDR2 for vs target analysis)",
            required=True
        ),
        SkillParameter(
            name="metric_group",
            is_multi=False,
            constrained_to="metric_groups",
            description="Metric group used to pull grouped metrics"
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
            constrained_values=["Y/Y", "P/P"],
            description="Growth type either Y/Y or P/P (ignored for vs target analysis)",
            default_value="Y/Y"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="calculated_metric_filters",
            description='This parameter allows filtering based on computed values like growth, delta, or share. The computed values are only available for metrics selected for this analysis. The available computations are growth, delta and share. It accepts a list of conditions, where each condition is a dictionary with:  metric: The metric being filtered. computation: The computation (growth, delta, share) operator: The comparison operator (">", "<", ">=", "<=", "between", "=="). value: The numeric threshold for filtering. If using "between", provide a list [min, max]. scale: the scale of value (percentage, bps, absolute)'
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value="Answer user question in 30 words or less using following facts:\n{{facts}}"
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value="Write a short headline followed by a 60 word or less paragraph about using facts below.\nUse the structure from the 2 examples below to learn how I typically write summary.\nBase your summary solely on the provided facts, avoiding assumptions or judgments.\nEnsure clarity and accuracy.\nUse markdown formatting for a structured and clear presentation.\n###\nPlease use the following as an example of good insights\nExample 1:\nFacts:\n[{'title': 'Peer Facts', 'facts': [{'level': 'Brand', 'competitor': 'PRIVATE LABEL', 'Sales': '$606,077,860.02', 'change': '$61,256,314.08', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 61256314.07799971}, {'level': 'Brand', 'competitor': 'GIOVANNI RANA', 'Sales': '$171,591,436.24', 'change': '$53,770,879.92', 'rank_curr': '3', 'rank_change': '+1', 'abs_diff': 53770879.92400019}, {'level': 'Brand', 'competitor': 'BARILLA', 'Sales': '$570,348,946.24', 'change': '$50,774,602.17', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 50774602.167000055}, {'level': 'Manufacturer', 'competitor': 'PRIVATE LABEL', 'Sales': '$606,077,860.02', 'change': '$61,256,314.08', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 61256314.07799971}, {'level': 'Manufacturer', 'competitor': 'PASTIFICIO RANA S.P.A.', 'Sales': '$171,591,436.24', 'change': '$53,770,879.92', 'rank_curr': '4', 'rank_change': '+1', 'abs_diff': 53770879.92400019}, {'level': 'Manufacturer', 'competitor': 'BARILLA G & R F.LLI S.P.A.', 'Sales': '$570,445,950.79', 'change': '$50,797,040.32', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 50797040.32000005}]}}, {'title': 'Breakout Facts', 'facts': []}, {'title': 'Subject Facts', 'facts': [{'metric': 'Sales', 'curr': '$2,344,866,829.62', 'prev': '$2,006,377,582.82', 'diff': '$338,489,246.79', 'growth': '16.87%', 'parent_metric': None, 'depth': 0}]}}, {'title': 'Notes about this answer', 'facts': [{'Note to the assistant:': 'The analysis was run using only these filters: Category = pasta'}, {'Note to the assistant:': 'Analysis ran for metric Sales filtered to pasta for Category for the period January 2022 to December 2022 vs January 2021 to December 2021'}]}, {'title': 'Metric Tree Facts', 'facts': [{'metric': 'Sales', 'curr': '$2,344,866,829.62', 'prev': '$2,006,377,582.82', 'diff': '$338,489,246.79', 'growth': '16.87%', 'parent_metric': None, 'depth': 0}, {'metric': 'ACV', 'curr': '3,614,315.41', 'prev': '3,919,522.06', 'diff': '-305,206.66', 'growth': '-7.79%', 'parent_metric': 'Sales', 'depth': 1}, {'metric': 'Volume', 'curr': '1,381,319,805.77', 'prev': '1,267,565,899.76', 'diff': '113,753,906.00', 'growth': '8.97%', 'parent_metric': 'Sales', 'depth': 1}]}]}.\nsummary:\n## Pasta Sales Analysis ##\n**Performance Overview:**\nThe pasta category has experienced significant increase in sales (+16.87% ), rising from $2,006,377,582.82 to $2,344,866,829.62..\n**Driving Metrics:**\nSales growth was driven by volume increase (+8.79%), while ACV declined by 7.79%, from 3,919,522.06 to 3,614,315.41.\n**Key Drivers:**\nPRIVATE LABEL led the market with $606,077,860.02 in sales, up by $61,256,314.08. GIOVANNI RANA and BARILLA followed, with sales increases of $53,770,879.92 and $50,774,602.17, respectively.\n###\nExample 2:\nFacts:\n[{'title': 'Peer Facts', 'facts': [{'level': 'Brand', 'competitor': 'PRIVATE LABEL', 'Sales': '$152,807,608.00', 'change': '-$453,271,875.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 453271875.0}, {'level': 'Brand', 'competitor': 'BARILLA', 'Sales': '$146,567,322.00', 'change': '-$423,781,691.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 423781691.0}, {'level': 'Brand', 'competitor': 'GIOVANNI RANA', 'Sales': '$41,702,602.00', 'change': '-$129,888,947.00', 'rank_curr': '3', 'rank_change': 'No Change', 'abs_diff': 129888947.0}]}}, {'title': 'Breakout Facts', 'facts': [{'level': 'Base Size', 'driver': '16 OUNCE', 'Sales': '$101,274,375.00', 'change': '-$306,205,018.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 306205018.0}, {'level': 'Base Size', 'driver': '12 OUNCE', 'Sales': '$24,372,801.00', 'change': '-$61,157,379.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 61157379.0}, {'level': 'Base Size', 'driver': '14.5 OUNCE', 'Sales': '$7,007,526.00', 'change': '-$16,631,014.00', 'rank_curr': '3', 'rank_change': 'No Change', 'abs_diff': 16631014.0}, {'level': 'Manufacturer', 'driver': 'BARILLA G & R F.LLI S.P.A.', 'Sales': '$146,567,322.00', 'change': '-$423,781,691.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 423781691.0}, {'level': 'Segment', 'driver': 'SHORT CUT', 'Sales': '$61,985,296.00', 'change': '-$185,709,862.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 185709862.0}, {'level': 'Segment', 'driver': 'LONG CUT', 'Sales': '$61,464,163.00', 'change': '-$177,343,077.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 177343077.0}, {'level': 'Segment', 'driver': 'BAKING', 'Sales': '$12,497,475.00', 'change': '-$34,233,200.00', 'rank_curr': '3', 'rank_change': 'No Change', 'abs_diff': 34233200.0}, {'level': 'Sub-Category', 'driver': 'SEMOLINA', 'Sales': '$117,014,563.00', 'change': '-$343,972,411.00', 'rank_curr': '1', 'rank_change': 'No Change', 'abs_diff': 343972411.0}, {'level': 'Sub-Category', 'driver': 'MULTIGRAIN', 'Sales': '$12,526,184.00', 'change': '-$31,841,841.00', 'rank_curr': '2', 'rank_change': 'No Change', 'abs_diff': 31841841.0}, {'level': 'Sub-Category', 'driver': 'REMAINING GRAIN', 'Sales': '$6,304,828.00', 'change': '-$18,091,439.00', 'rank_curr': '4', 'rank_change': '-1', 'abs_diff': 18091439.0}]}}, {'title': 'Subject Facts', 'facts': [{'metric': 'Sales', 'curr': '$146,567,322.00', 'prev': '$570,349,013.00', 'diff': '-$423,781,691.00', 'growth': '-74.30%', 'parent_metric': None, 'depth': 0}]}}, {'title': 'Notes about this answer', 'facts': [{'Note to the assistant:': 'The analysis was run using only these filters: Brand = barilla, Category = pasta, Country = united states'}]}, {'title': 'Metric Tree Facts', 'facts': [{'metric': 'Sales', 'curr': '$146,567,322.00', 'prev': '$570,349,013.00', 'diff': '-$423,781,691.00', 'growth': '-74.30%', 'parent_metric': None, 'depth': 0}, {'metric': 'Volume', 'curr': '90,622,621.00', 'prev': '353,012,269.00', 'diff': '-262,389,648.00', 'growth': '-74.33%', 'parent_metric': 'Sales', 'depth': 1}, {'metric': 'Units', 'curr': '89,526,220.00', 'prev': '345,124,705.00', 'diff': '-255,598,485.00', 'growth': '-74.06%', 'parent_metric': 'Sales', 'depth': 1}]}]}.\nsummary:\n## Barilla Sales Analysis ##\n**Performance Overview:**\nBarilla's sales witnessed a -74.30% decline, dropping from $570,349,013.00 to $146,567,322.00. This contraction is slightly below the benchmark set by competitors like PRIVATE LABEL and GIOVANNI RANA,\n**Driving Metrics:**\nSales for Barilla plummeted by -74.30%, driven by substantial declines in volume (-74.33%, from 353,012,269 to 90,622,621) and units (-74.06%, from 345,124,705 to 89,526,220).\n**Key Drivers:**\nBase Size adjustments with the 16 OUNCE package experiencing the most significant sales drop of $306,205,018.00.\nManufacturer insights reveal Barilla G & R F.LLI S.P.A. as the most affected, paralleling the brand's own sales contraction.\nSegment analysis points out the SHORT CUT and LONG CUT as leading categories in sales decline, with SHORT CUT experiencing a $185,709,862.00 drop.\nSub-Category trends show SEMOLINA and MULTIGRAIN as heavily impacted, with SEMOLINA sales down by $343,972,411.00.\n###\nFacts:\n{{facts}}\nSummary:"
        ),
        SkillParameter(
            name="table_viz_layout",
            parameter_type="visualization",
            description="Table Viz Layout",
            default_value=default_table_layout
        )
    ]
)
def sixt_plan_drivers(parameters: SkillInput):
    param_dict = {"periods": [], "metric": "", "metric_group": "", "limit_n": 10, "breakouts": None, "growth_type": "Y/Y", "other_filters": [], "calculated_metric_filters": None}
    print(f"DEBUG: sixt_plan_drivers received parameters: {parameters.arguments}")
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    print(f"DEBUG: Processed param_dict: {param_dict}")
    env = SimpleNamespace(**param_dict)
    
    print(f"DEBUG: About to run SixtMetricDriverTemplateParameterSetup with metric: {env.metric}")
    SixtMetricDriverTemplateParameterSetup(env=env)
    
    print(f"DEBUG: Creating SixtMetricDriver from env")
    env.da = SixtMetricDriver.from_env(env=env)

    print(f"DEBUG: About to run driver analysis")
    _ = env.da.run_from_env()

    optional_columns = ["vs Target"] if check_vs_enabled([env.metric]) else []
    results = env.da.get_display_tables(optional_columns=optional_columns)

    tables = {
        "Metrics": results['viz_metric_df']
    }
    tables.update(results['viz_breakout_dfs'])

    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.da.paramater_display_infomation.items()]

    insights_dfs = [env.da.df_notes, env.da.breakout_facts, env.da.subject_fact.get("df", pd.DataFrame())]

    warning_messages = env.da.get_warning_messages()

    viz, insights, final_prompt, export_data = render_layout(tables,
                                                            env.da.title,
                                                            env.da.subtitle,
                                                            insights_dfs,
                                                            warning_messages,
                                                            parameters.arguments.max_prompt,
                                                            parameters.arguments.insight_prompt,
                                                            parameters.arguments.table_viz_layout)

    # Add trend charts for vs target metrics
    if check_vs_enabled([env.metric]):
        print(f"DEBUG: Creating trend charts for vs target metric")
        trend_vizs = create_trend_chart(env, insights)
        if trend_vizs:
            if isinstance(trend_vizs, list):
                viz.extend(trend_vizs)  # Add multiple charts
            else:
                viz.append(trend_vizs)  # Add single chart (fallback)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=[],
        export_data=[ExportData(name=name, data=df) for name, df in export_data.items()]
    )

def analyze_supporting_metrics_correlation(df, current_year, previous_year, metrics):
    """Analyze correlation between supporting metrics changes and DDR performance"""
    if df is None or df.empty:
        return "No trend data available for correlation analysis."
    
    try:
        # Calculate YoY changes for each supporting metric
        insights = []
        
        print(f"**zz DEBUG: Correlation analysis DF columns: {df.columns.tolist()}")
        print(f"**zz DEBUG: Correlation analysis DF shape: {df.shape}")
        print(f"**zz DEBUG: Sample data from DF: {df.head()}")
        
        # Group by metric and calculate yearly averages
        yearly_averages = {}
        
        for metric in metrics:
            if metric in df.columns:
                # Filter data by year (assuming date_column or period info is available)
                current_data = df[df['month'].str.contains(current_year, na=False)] if 'month' in df.columns else df
                previous_data = df[df['month'].str.contains(previous_year, na=False)] if 'month' in df.columns else pd.DataFrame()
                
                if not current_data.empty:
                    current_avg = current_data[metric].mean()
                    yearly_averages[f"{metric}_{current_year}"] = current_avg
                    
                    if not previous_data.empty:
                        previous_avg = previous_data[metric].mean()
                        yearly_averages[f"{metric}_{previous_year}"] = previous_avg
                        
                        # Calculate YoY change
                        yoy_change = ((current_avg - previous_avg) / previous_avg * 100) if previous_avg != 0 else 0
                        yearly_averages[f"{metric}_yoy_change"] = yoy_change
                        
                        print(f"**zz DEBUG: {metric} - {previous_year}: {previous_avg:.3f}, {current_year}: {current_avg:.3f}, YoY: {yoy_change:.1f}%")
        
        # Generate correlation insights based on business logic
        correlation_text = generate_correlation_insights(yearly_averages, current_year, previous_year)
        
        print(f"**zz DEBUG: Generated correlation text: {correlation_text}")
        return correlation_text
        
    except Exception as e:
        print(f"**zz DEBUG: Error in correlation analysis: {e}")
        import traceback
        print(f"**zz DEBUG: Full traceback: {traceback.format_exc()}")
        return "Unable to perform correlation analysis on supporting metrics."

def generate_correlation_insights(yearly_averages, current_year, previous_year):
    """Generate business insights from YoY correlation analysis"""
    print(f"**zz DEBUG: yearly_averages keys: {list(yearly_averages.keys())}")
    print(f"**zz DEBUG: yearly_averages values: {yearly_averages}")
    insights = []
    
    # Check for significant changes in key metrics
    key_insights = {
        'checkin_count': 'Check-in volume',
        'damage_at_check_in': 'Damage detection efficiency', 
        'months_maturity_employee': 'Employee experience',
        'live_check_in_rate': 'Process digitalization'
    }
    
    for metric, description in key_insights.items():
        yoy_key = f"{metric}_yoy_change"
        if yoy_key in yearly_averages:
            change = yearly_averages[yoy_key]
            
            if abs(change) > 5:  # Significant change threshold
                direction = "increased" if change > 0 else "decreased"
                insights.append(f"• {description} {direction} by {abs(change):.1f}% from {previous_year} to {current_year}")
    
    # Add DDR correlation context
    insights.append(f"\n**Root Cause Analysis ({current_year} vs {previous_year}):**")
    
    # Business logic connections based on the framework documents
    if 'months_maturity_employee_yoy_change' in yearly_averages:
        maturity_change = yearly_averages['months_maturity_employee_yoy_change']
        if maturity_change > 10:
            insights.append("• Increased employee maturity may contribute to improved damage detection capabilities")
        elif maturity_change < -10:
            insights.append("• Decreased employee maturity could negatively impact damage detection performance")
    
    if 'damage_at_check_in_yoy_change' in yearly_averages:
        detection_change = yearly_averages['damage_at_check_in_yoy_change']
        if detection_change > 5:
            insights.append("• Higher damage detection rate at check-in supports better DDR performance")
        elif detection_change < -5:
            insights.append("• Lower damage detection efficiency may be limiting DDR achievement vs target")
    
    if 'live_check_in_rate_yoy_change' in yearly_averages:
        digital_change = yearly_averages['live_check_in_rate_yoy_change']
        if digital_change > 10:
            insights.append("• Increased digitalization of check-in process may enhance damage detection accuracy")
    
    return "\n".join(insights) if insights else "No significant year-over-year changes detected in supporting metrics."

def create_trend_chart(env, insights=None):
    """Create monthly trend chart for supporting metrics using AdvanceTrend"""
    print(f"DEBUG: Creating trend chart with periods: {env.periods}")
    
    # Extract year from periods - use first period and get full year
    if env.periods and len(env.periods) > 0:
        period = env.periods[0]
        # Extract year from period (e.g., "2019" or "q2 2019")
        if period.isdigit():
            current_year = period
        else:
            # Extract year from formatted period
            current_year = period.split()[-1] if ' ' in period else period[-4:]
    else:
        current_year = "2019"  # fallback
    
    # Also get previous year for YoY comparison
    previous_year = str(int(current_year) - 1)
    
    print(f"DEBUG: Using current year {current_year} and previous year {previous_year} for trend analysis")
    
    # Define supporting metrics for trend analysis
    trend_metrics = [
        'checkin_count',
        'damage_at_check_in', 
        'months_maturity_employee',
        'live_check_in_rate'
    ]
    
    # Create trend environment with monthly periods for both years
    current_year_periods = []
    previous_year_periods = []
    
    for month in range(1, 13):
        month_name = calendar.month_name[month].lower()[:3]  # jan, feb, etc.
        current_year_periods.append(f"{month_name} {current_year}")
        previous_year_periods.append(f"{month_name} {previous_year}")
    
    # Combine both years for comprehensive analysis
    all_periods = previous_year_periods + current_year_periods
    
    # Create trend environment 
    trend_env = SimpleNamespace()
    trend_env.periods = all_periods  # Use both years
    trend_env.metrics = trend_metrics
    trend_env.breakouts = []
    trend_env.growth_type = "None"  # No growth for supporting metrics
    trend_env.other_filters = env.other_filters if hasattr(env, 'other_filters') else []
    trend_env.time_granularity = "month"  # Monthly granularity
    trend_env.limit_n = 10
    
    print(f"DEBUG: Creating AdvanceTrend with periods for both years: {len(all_periods)} periods")
    
    try:
        # Set up trend analysis
        TrendTemplateParameterSetup(env=trend_env)
        trend_analysis = AdvanceTrend.from_env(env=trend_env)
        df = trend_analysis.run_from_env()
        
        print(f"DEBUG: Trend analysis DF shape: {df.shape if df is not None else 'None'}")
        
        # Get chart variables for all chart types
        charts = trend_analysis.get_dynamic_layout_chart_vars()
        
        # Create multiple visualizations for all chart types (absolute, growth, difference)
        if charts:
            viz_list = []
            
            # Prepare base variables for chart layout
            combined_insights = insights if insights else ""
            
            # Create a visualization for each chart type
            for chart_name, chart_vars in charts.items():
                tab_vars = {
                    "headline": f"Supporting Metrics Trends - {current_year}",
                    "sub_headline": f"Monthly trend analysis - {chart_name}",
                    "hide_growth_warning": True,
                    "exec_summary": combined_insights,
                    "warning": []
                }
                
                chart_vars["footer"] = f"*{chart_vars.get('footer', 'Monthly trend data')}"
                
                # Render chart using default trend chart layout
                rendered = wire_layout(json.loads(default_trend_chart_layout), {**tab_vars, **chart_vars})
                viz_list.append(SkillVisualization(title=f"Supporting Metrics - {chart_name}", layout=rendered))
            
            return viz_list
        else:
            print("DEBUG: No charts generated from trend analysis")
            return None
            
    except Exception as e:
        print(f"DEBUG: Error creating trend chart: {e}")
        return None

def render_layout(tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt, viz_layout):
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

    general_vars = {"headline": title if title else "Total",
                    "sub_headline": subtitle if subtitle else "Driver Analysis",
                    "hide_growth_warning": False if warnings else True,
                    "exec_summary": insights if insights else "No Insights.",
                    "warning": warnings}

    for name, table in tables.items():
        export_data[name] = table
        hide_footer = True
        table_vars = get_table_layout_vars(table, sparkline_col="sparkline")
        table_vars["hide_footer"] = hide_footer
        rendered = wire_layout(json.loads(viz_layout), {**general_vars, **table_vars})
        viz_list.append(SkillVisualization(title=name, layout=rendered))

    return viz_list, insights, max_response_prompt, export_data

# SIXT UTILITIES
class SixtTestColumnNames(Enum):
    '''
    Based on the Sixt dataset
    '''

    # Metrics
    CHECKIN_COUNT = "checkin_count"
    DAMAGE_COUNT = "damage_count"
    DAMAGE_DETECTED_AT_CHECKIN_FLG = "damage_detected_at_checkin_flg"
    HIGH_POTENTIALS_COUNT = "high_potentials_count"
    LIVE_CHECKIN_FLG = "live_checkin_flg"
    MONTHS_MATURITY_EMPLOYEE = "months_maturity_employee"
    TARGET_DDR1 = "target_ddr1"
    TARGET_DDR2 = "target_ddr2"
    DAMAGE_AT_CHECK_IN = "damage_at_check_in"
    DDR1 = "ddr1"
    DDR2 = "ddr2"
    LIVE_CHECK_IN_RATE = "live_check_in_rate"
   
    # Grouped Metrics
    DAMAGE_DETECTION_GROUP = "damage_detection"

    # Categorical Dimensions
    BRANCH_TYPE_CONSOLIDATED = "branch_type_consolidated"
    BRNC_NAME = "brnc_name"
    BRNC_POOL = "brnc_pool"
    BRNC_REGION = "brnc_region"
    BRNCH_FIR_NAME = "brnch_fir_name"
    ID_RFP = "id_rfp"
    MANAM_SANITIZED = "manam_sanitized"
    PRODUCT = "product"
    PROFILES_IN_BRANCH = "profiles_in_branch"

    # Example dimension values
    BRANCH_TYPE__AIRPORT = "Airport"
    BRANCH_TYPE__DOWNTOWN = "Downtown"
    BRANCH_TYPE__RAILWAY = "Railway"
    BRNC_REGION__EUROPE = "Europe"
    PRODUCT__PASSENGER_CAR_LONG_TERM = "Passenger Car / Long Term"
    PRODUCT__PASSENGER_CAR_SHORT_TERM = "Passenger Car / Short Term"
    PRODUCT__VAN_TRUCK_LONG_TERM = "Van/Truck / Long Term"
    PRODUCT__VAN_TRUCK_SHORT_TERM = "Van/Truck / Short Term"
    PROFILES__DIFFERENT_ROLES = "DIFFERENT ROLES IN BRANCH"
    PROFILES__ONLY_RSA_ROLES = "ONLY RSA ROLES"

    # Date Dimension
    MAX_TIME_DATE = "max_time_date"
    MONTH = "max_time_month"
    QUARTER = "max_time_quarter"
    YEAR = "max_time_year"

VS_ENABLED_METRICS = [SixtTestColumnNames.DDR1.value, SixtTestColumnNames.DDR2.value]

def check_vs_enabled(metrics):
    if all([metric in VS_ENABLED_METRICS for metric in metrics]):
        return True
    
    return False

# SIXT METRIC DRIVER CLASSES
class SixtMetricTreeAnalysis(MetricTreeAnalysis):
    """
    Metric tree analysis for Sixt
    """
    def __init__(self, sql_exec:Connector=None, df_provider=None, sp=None):
        super().__init__(sql_exec, df_provider, sp)
    
    def run(self, table, metrics, period_filters, query_filters=[], table_specific_filters={}, driver_metrics=[], view="", include_sparklines=True, two_year_filter=None, period_col_granularity='day', metric_props={}, add_impacts=False, impact_formulas={}):
        print(f"DEBUG: SixtMetricTreeAnalysis.run called with metrics: {metrics}")
        print(f"DEBUG: period_filters: {period_filters}")
        print(f"DEBUG: check_vs_enabled result: {check_vs_enabled(metrics)}")
        
        # For vs target metrics, ensure we have two period filters to prevent IndexError
        modified_period_filters = period_filters
        if check_vs_enabled(metrics) and len(period_filters) == 1:
            print(f"DEBUG: Adding duplicate period filter for vs target metrics")
            # Duplicate the current period filter to prevent IndexError
            modified_period_filters = period_filters + period_filters
        
        metric_df = super().run(table, metrics, modified_period_filters, query_filters, table_specific_filters, driver_metrics, view, include_sparklines, two_year_filter, period_col_granularity, metric_props, add_impacts, impact_formulas)
        
        if not check_vs_enabled(metrics):
            print(f"DEBUG: Not vs enabled metrics, returning standard metric_df")
            return metric_df
        
        print(f"DEBUG: Adding vs Target column for metrics: {metrics}")
        additional_filters = table_specific_filters.get('default', [])
        target_metrics = [f"target_{metric}" for metric in metrics]
        target_metrics = [self.helper.get_metric_prop(m, metric_props) for m in target_metrics]
        print(f"DEBUG: Target metrics to pull: {target_metrics}")
        
        try:
            target_df = self.pull_data_func(metrics=target_metrics, filters=query_filters+additional_filters+[period_filters[0]])
            print(f"DEBUG: Target data retrieved successfully")
            print(f"DEBUG: Target df shape: {target_df.shape}")
            print(f"DEBUG: Target df columns: {target_df.columns.tolist()}")

            # For vs target metrics, set prev to target value and calculate difference
            print(f"DEBUG: Setting prev column to target values for vs target metrics")
            for metric in metrics:
                metric_df.loc[metric, 'prev'] = target_df[f"target_{metric}"].iloc[0]
                metric_df.loc[metric, 'diff'] = metric_df.loc[metric, 'curr'] - target_df[f"target_{metric}"].iloc[0]
            
            metric_df['growth'] = 0  # No growth calculation for vs target

            metric_df['vs Target'] = metric_df.apply(
                lambda row: row['curr'] - target_df[f"target_{row.name}"].iloc[0], 
                axis=1
            )
            print(f"DEBUG: Added vs Target column successfully")
        except Exception as e:
            print(f"DEBUG: Error adding vs Target column: {e}")
            raise

        return metric_df

class SixtBreakoutDrivers(BreakoutDrivers):
    """
    Breakout drivers for Sixt
    """
    def __init__(self, dim_hierarchy, dim_val_map={}, sql_exec:Connector=None, df_provider=None, sp=None):
        super().__init__(dim_hierarchy, dim_val_map, sql_exec, df_provider, sp)

    def run(self, table, metric, breakouts, period_filters, query_filters=[], table_specific_filters={}, top_n=5, include_sparklines=True, two_year_filter=None, period_col_granularity='day', view="", growth_type="", metric_props={}, dim_props={}):
        print(f"DEBUG: SixtBreakoutDrivers.run called with metric: {metric}")
        print(f"DEBUG: period_filters length: {len(period_filters)}")
        
        # For vs target metrics, ensure we have two period filters to prevent IndexError
        modified_period_filters = period_filters
        if check_vs_enabled([metric]) and len(period_filters) == 1:
            print(f"DEBUG: Adding duplicate period filter for vs target metric")
            # Duplicate the current period filter to prevent IndexError
            modified_period_filters = period_filters + period_filters
        
        breakout_df = super().run(table, metric, breakouts, modified_period_filters, query_filters, table_specific_filters, top_n, include_sparklines, two_year_filter, period_col_granularity, view, growth_type, metric_props, dim_props)
        
        if not check_vs_enabled([metric]):
            return breakout_df
        
        # Add vs Target column and set target values
        print(f"DEBUG: Adding vs Target column for breakouts")
        additional_filters = table_specific_filters.get('default', [])
        target_metric = f"target_{metric}"
        target_metric = self.helper.get_metric_prop(target_metric, metric_props)
        dfs = []
        for breakout in breakouts:
            
            target_df = self.pull_data_func(metrics=[target_metric], breakouts=[breakout], filters=query_filters+additional_filters+[period_filters[0]])
            target_df.set_index(breakout, inplace=True)
            target_df.index.name = 'dim_value'
            target_df.index = target_df.index.astype(str)
            dfs.append(target_df)
        target_df = pd.concat(dfs)

        # For vs target metrics, set prev to target value and calculate difference
        print(f"DEBUG: Setting prev column to target values for vs target breakouts")
        breakout_df['prev'] = breakout_df.apply(
            lambda row: target_df[target_df.index == row.name][f"target_{metric}"].iloc[0], 
            axis=1
        )
        breakout_df['diff'] = breakout_df.apply(
            lambda row: row['curr'] - target_df[target_df.index == row.name][f"target_{metric}"].iloc[0], 
            axis=1
        )
        breakout_df['diff_pct'] = 0  # No growth percentage for vs target
        breakout_df['rank_change'] = 0

        breakout_df['vs Target'] = breakout_df['diff']  # Same as diff column

        return breakout_df

class SixtMetricDriver(DriverAnalysis):
    """
    Driver for Sixt metric analysis
    """
    def __init__(self, dim_hierarchy, dim_val_map={}, sql_exec: Connector=None, constrained_values={}, compare_date_warning_msg=None, df_provider=None, sp=None):
        super().__init__(dim_hierarchy, dim_val_map, sql_exec, constrained_values, compare_date_warning_msg, df_provider, sp)
        self.mta = SixtMetricTreeAnalysis(sql_exec, df_provider, sp)
        self.ba = SixtBreakoutDrivers(dim_hierarchy, dim_val_map, sql_exec, df_provider, sp)

    def get_display_tables(self, optional_columns=[]):
        metric_df = self._metric_df.copy()
        breakout_df = self._breakout_df.copy()

        # Define required columns for metric_df
        metric_tree_required_columns = ["curr", "prev", "diff", "growth"] + optional_columns
        if self.include_sparklines:
            metric_tree_required_columns.append("sparkline")

        if "impact" in metric_df.columns:
            metric_tree_required_columns.append("impact")

        # Filter metric_df to include only the required columns
        metric_df = metric_df[metric_tree_required_columns]

        # Apply formatting for metric_df
        for col in ["curr", "prev", "diff", "growth"] + optional_columns:
            metric_df[col] = metric_df.apply(
                lambda row: self.helper.get_formatted_num(
                    row[col],
                    self.helper.get_metric_prop(row.name, self.metric_props).get("fmt",
                                                                                 "") if col != "growth" else self.helper.get_metric_prop(
                        row.name, self.metric_props).get("growth_fmt", "")
                ), axis=1
            )

        if "impact" in metric_df.columns:
            metric_df["impact"] = metric_df.apply(
                lambda row: self.helper.get_formatted_num(row["impact"], self.mta.impact_format), axis=1
            )

        # rename columns - use different label for vs target metrics
        # Check if we have vs Target column to determine if this is vs target analysis
        if 'vs Target' in metric_df.columns:
            metric_df = metric_df.rename(
                columns={'curr': 'Value', 'prev': 'Target', 'diff': 'vs Target', 'growth': '% Growth'})
        else:
            metric_df = metric_df.rename(
                columns={'curr': 'Value', 'prev': 'Prev Value', 'diff': 'Change', 'growth': '% Growth'})
        
        metric_df = metric_df.reset_index()

        # rename index to metric labels
        metric_df["index"] = metric_df["index"].apply(lambda x: self.helper.get_metric_prop(x, self.metric_props).get("label", x))

        # indent non target metric
        metric_df["index"] = metric_df["index"].apply(lambda x: f"  {x}" if x != self.mta.target_metric else x)

        metric_df = metric_df.rename(columns={"index": ""})

        # Define required columns for breakout_df
        breakout_required_columns = ["curr", "prev", "diff", "diff_pct", "rank_change"] + optional_columns
        if self.include_sparklines:
            breakout_required_columns.append("sparkline")

        breakout_dfs = {}

        # Apply formatting for breakout_df
        for col in ["curr", "prev", "diff", "diff_pct"] + optional_columns:
            breakout_df[col] = breakout_df.apply(
                lambda row: self.helper.get_formatted_num(row[col],
                                                          self.ba.target_metric["fmt"] if col != "diff_pct" else
                                                          self.ba.target_metric["growth_fmt"]),
                axis=1
            )

        # Format rank column
        breakout_df["rank_curr"] = breakout_df["rank_curr"]
        breakout_df["rank_change"] = breakout_df.apply(lambda row: f"{int(row['rank_curr'])} ({fmt_sign_num(row['rank_change'])})"
                                                    if (row['rank_change'] and pd.notna(row['rank_change']) and row['rank_change'] != 0)
                                                    else row['rank_curr'], axis=1)
        breakout_df = breakout_df.reset_index()

        breakout_dims = list(breakout_df["dim"].unique())
        if self.ba.dim_hier:
            # display according to the dim hierarchy ordering
            ordering_dict = {value: index for index, value in enumerate(self.ba.dim_hier.get_hierarchy_ordering())}
            # rename cols to dim labels
            ordering_dict = {self.helper.get_dimension_prop(k, self.dim_props).get("label", k): v for k, v in ordering_dict.items()}
            # sort dims by hierarchy order
            breakout_dims.sort(key=lambda x: (ordering_dict.get(x, len(ordering_dict)), x))

        comp_dim = None
        if self.ba._owner_dim:
            comp_dim = next((d for d in breakout_dims if d.lower() == self.ba._owner_dim.lower()), None)

        if comp_dim:
            breakout_dims = [comp_dim] + [x for x in breakout_dims if x != comp_dim]

        for dim in breakout_dims:
            b_df = breakout_df[breakout_df["dim"] == dim]
            if str(dim).lower() == str(comp_dim).lower():
                viz_name = "Benchmark"
            else:
                viz_name = dim
            b_df = b_df.rename(columns={'dim_value': dim})
            b_df = b_df[[dim] + breakout_required_columns]

            # rename columns - use different label for vs target metrics
            # For breakouts, we can check if any vs Target column exists
            if 'vs Target' in b_df.columns:
                b_df = b_df.rename(
                    columns={'curr': 'Value', 'prev': 'Target', 'diff': 'vs Target', 'diff_pct': '% Growth',
                             'rank_change': 'Rank Change'})
            else:
                b_df = b_df.rename(
                    columns={'curr': 'Value', 'prev': 'Prev Value', 'diff': 'Change', 'diff_pct': '% Growth',
                             'rank_change': 'Rank Change'})
            breakout_dfs[viz_name] = b_df

        return {"viz_metric_df": metric_df, "viz_breakout_dfs": breakout_dfs}

class SixtMetricDriverTemplateParameterSetup(DriverAnalysisTemplateParameterSetup):
    """
    Template parameter setup for Sixt metric driver
    """
    def __init__(self, env=None):
        super().__init__(env=env)
    
    def map_env_values(self, env=None):
        if env is None:
            raise exit_with_status("env namespace is required.")

        driver_analysis_parameters = {}
        pills = {}

        ## Setup DB

        database_id = self.dataset_metadata.get("database_id")
        driver_analysis_parameters["table"] = self.dataset_metadata.get("sql_table")
        driver_analysis_parameters["derived_sql_table"] = self.dataset_metadata.get("derived_table_sql") or ""
        dataset_misc_info = self.dataset_metadata.get("misc_info") or {}
        driver_analysis_parameters["impact_formulas"] = dataset_misc_info.get("impact_formulas") or {}

        driver_analysis_parameters["con"] = Connector("db", database_id=database_id, sql_dialect=self.dataset_metadata.get("sql_dialect"), limit=self.sql_row_limit)

        _, driver_analysis_parameters["dim_hierarchy"] = self.sp.data.get_dimension_hierarchy()
        _, driver_analysis_parameters["driver_metrics"] = self.sp.data.get_metric_hierarchy()
        driver_analysis_parameters["constrained_values"] = self.constrained_values

        ## Map Env Variables

        # Get metric_props, dim_props, setting on env since the chart templates reference these
        env.metric_props = self.get_metric_props()
        env.dim_props = self.get_dimension_props()

        # Get metrics and metric pills

        driver_analysis_parameters["metric"] = env.metric
        metric_pills = self.get_metric_pills(env.metric, env.metric_props)

        # Get filters by dimension
        driver_analysis_parameters["query_filters"], query_filters_pills = self.parse_dimensions(env)

        # Parse breakout dims to the sql columns
        driver_analysis_parameters["breakouts"], breakout_pills = self.parse_breakout_dims(env.breakouts)

        # guardrails for unsupported calculated filters
        calculated_metric_filters = env.calculated_metric_filters if hasattr(env, "calculated_metric_filters") else None
        query, llm_notes, _, _ = self.get_metric_computation_filters([env.metric], calculated_metric_filters, "None", env.metric_props)
        if query:
            self.get_unsupported_filter_message(llm_notes, 'metric drivers')

        ### Period Handling ###

        default_granularity = self.dataset_metadata.get("default_granularity")
        compare_date_warning_msg = None

        if not self.is_period_table:
            start_date, end_date, comp_start_date, comp_end_date = self.handle_periods_and_comparison_periods(env.periods, env.growth_type, allowed_tokens=['<no_period_provided>', '<since_launch>'])

            # date/period column metadata. Assumes the date column is a date type
            period_col = env.date_col if hasattr(env, "date_col") and env.date_col else self.get_period_col()

            if not period_col:
                exit_with_status("A date column must be provided.")

            # create period filters using start date and end date, and comparison start and end dates
            period_filters = []

            if start_date and end_date:
                period_filters.append(
                    { "col": period_col, "op": "BETWEEN", "val": f"'{start_date}' AND '{end_date}'"}
                )

            # Only add comparison period filters if NOT using vs target comparison
            if comp_start_date and comp_end_date and not check_vs_enabled([env.metric]):
                print(f"DEBUG: Adding comparison period filter for non-vs-target metric")
                period_filters.append(
                    { "col": period_col, "op": "BETWEEN", "val": f"'{comp_start_date}' AND '{comp_end_date}'" }
                )
                if self.is_date_range_completely_out_of_bounds(comp_start_date, comp_end_date):
                    time_granularity = self.dataset_metadata.get("default_granularity")
                    start_period = self.helper.format_date_from_time_granularity(self.dataset_metadata["min_date"], time_granularity)
                    end_period = self.helper.format_date_from_time_granularity(self.dataset_metadata["max_date"], time_granularity)
                    msg = [f"Please inform the user that the analysis cannot run because data is unavailable for the required {env.growth_type} comparison period."]
                    msg.append(f"Data is only available from {start_period} to {end_period}.")
                    msg.append(f"Ask the user to modify the date range to ensure it aligns with an available {env.growth_type} comparison period within this timeframe.")
                    msg.append("Please do not make any assumptions on behalf of the user.")
                    exit_with_status(" ".join(msg))
                elif self.is_date_range_partially_out_of_bounds(comp_start_date, comp_end_date):
                    compare_date_warning_msg = "Data is only avaiable for partial comparison period. This gap might impact the analysis results and insights."
            elif check_vs_enabled([env.metric]):
                print(f"DEBUG: Skipping comparison period for vs target metric: {env.metric}")
                comp_start_date = None
                comp_end_date = None

            two_year_filter = None

            # format dates after adding them to the period filters

            start_date = self.helper.format_date_from_time_granularity(start_date, default_granularity)
            end_date = self.helper.format_date_from_time_granularity(end_date, default_granularity)
            comp_start_date = self.helper.format_date_from_time_granularity(comp_start_date, default_granularity)
            comp_end_date = self.helper.format_date_from_time_granularity(comp_end_date, default_granularity)

            date_labels = {"start_date": start_date, "end_date": end_date, "compare_start_date": comp_start_date, "compare_end_date": comp_end_date}

        else:
            _, periods_in_year = self.sp.data.get_periods_in_year()
            date_filters, _ = self.get_time_variables(env.periods)
            period_filters, date_labels = self.get_period_filters(sql_con=driver_analysis_parameters["con"],
                                                        date_filters=date_filters,
                                                        growth_type=env.growth_type,
                                                        sparkline_n_year=2)

            if period_filters:
                two_year_filter = period_filters[-1]
                period_filters = period_filters[:-1]
            else:
                two_year_filter = None

            print("period_filters")
            print(period_filters)

            # Skip Y/Y comparison validation for vs target metrics
            if str(env.growth_type).lower() != "none" and not date_labels.get("compare_start_date") and not check_vs_enabled([env.metric]):
                msg = ["Please inform the user that the analysis cannot run because data is unavailable for the required year-over-year (Y/Y) comparison period."]
                msg.append(f"Data is only available from {date_labels.get('data_start_date')} to {date_labels.get('data_end_date')}.")
                msg.append(f"Ask the user to modify the date range to ensure it aligns with an available {env.growth_type} comparison period within this timeframe.")
                msg.append("Please do not make any assumptions on behalf of the user.")
                exit_with_status(" ".join(msg))
            elif self.is_period_date_partially_out_of_bounds(period_filters):
                compare_date_warning_msg = "Data is only avaiable for partial comparison period. This gap might impact the analysis results and insights."

            start_date = date_labels.get("start_date")
            end_date = date_labels.get("end_date")
            comp_start_date = date_labels.get("compare_start_date")
            comp_end_date = date_labels.get("compare_end_date")

        # Set the trend date parameters
        driver_analysis_parameters["date_labels"] = date_labels
        driver_analysis_parameters["period_filters"] = period_filters
        driver_analysis_parameters["period_col_granularity"] = "day"
        driver_analysis_parameters["two_year_filter"] = two_year_filter
        driver_analysis_parameters["compare_date_warning_msg"] = compare_date_warning_msg

        # convert limit_n to an int
        if hasattr(env, "limit_n") and env.limit_n:
            if env.limit_n == NO_LIMIT_N:
                driver_analysis_parameters["limit_n"] = None
            else:
                driver_analysis_parameters["limit_n"] = self.convert_to_int(env.limit_n)

        # set growth type - default to None for vs target metrics
        if check_vs_enabled([env.metric]):
            print(f"DEBUG: Setting growth_type to None for vs target metric: {env.metric}")
            driver_analysis_parameters["growth_type"] = "None"
            env.growth_type = "None"  # Also set on env to prevent comparison period logic
        else:
            print(f"DEBUG: Using standard growth_type: {env.growth_type}")
            driver_analysis_parameters["growth_type"] = env.growth_type

        # use sparklines

        env.include_sparklines = True # must be set since the chart references env.include_sparklines
        driver_analysis_parameters["include_sparklines"] = env.include_sparklines

        ## add UI bubbles

        if metric_pills:
            pills["metric"] = f"Metric: {self.helper.and_comma_join(metric_pills)}"
        if query_filters_pills:
            pills["filters"] = f"Filter: {self.helper.and_comma_join(query_filters_pills)}"
        if breakout_pills:
            pills["breakout"] = f"Breakout: {self.helper.and_comma_join(breakout_pills)}"
        if start_date and end_date:
            if start_date == end_date:
                pills["period"] = f"Period: {start_date}"
            else:
                pills["period"] = f"Period: {start_date} to {end_date}"
        # Only show compare period if not using target comparison
        if comp_start_date and comp_end_date and not check_vs_enabled([env.metric]):
            if comp_start_date == comp_end_date:
                pills["compare_period"] = f"Compare Period: {comp_start_date}"
            else:
                pills["compare_period"] = f"Compare Period: {comp_start_date} to {comp_end_date}"
        elif check_vs_enabled([env.metric]):
            pills["comparison"] = "vs Target"
        if hasattr(env, "growth_type"):
            if str(env.growth_type).lower() in ["p/p", "y/y"] and not check_vs_enabled([env.metric]):
                pills["growth_type"] = f"Growth Type: {str(env.growth_type)}"

        driver_analysis_parameters["ParameterDisplayDescription"] = pills

        ## Set the driver analysis parameters
        env.driver_analysis_parameters = driver_analysis_parameters

if __name__ == '__main__':
    skill_input: SkillInput = sixt_plan_drivers.create_input(
        arguments={
            "breakouts": [
                "brnc_name"
            ],
            "metric": "ddr1",
            "periods": [
                "2019"
            ]
        }
    )
    out = sixt_plan_drivers(skill_input)
    preview_skill(sixt_plan_drivers, out)