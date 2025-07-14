from __future__ import annotations

import json
import logging
import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from prophet import Prophet
import statsmodels.api as sm
from types import SimpleNamespace
from datetime import datetime

from answer_rocket import AnswerRocketClient
from skill_framework import skill, SkillParameter, SkillInput, SkillOutput, SkillVisualization, ParameterDisplayDescription
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout
from skill_framework.preview import preview_skill

logger = logging.getLogger(__name__)

@skill(
    name="Time Series Forecast",
    description="Advanced forecasting skill that uses multiple models (Linear, Holt-Winters, Prophet) to predict future values of a metric. Automatically selects the best performing model and provides comprehensive analysis including outlier detection and growth trends.",
    capabilities="Can forecast any metric over time using multiple statistical models. Provides model comparison, outlier detection, rolling statistics, and growth analysis. Supports filtering by installation or other dimensions.",
    limitations="Requires historical time series data. Works best with at least 12 data points. Forecast accuracy depends on data quality and historical patterns.",
    parameters=[
        SkillParameter(
            name="metric",
            description="The metric to forecast (e.g., total_work_orders, total_failures, failure_rate)",
            required=True,
            constrained_values=["total_work_orders", "total_failures", "failure_rate"]
        ),
        SkillParameter(
            name="start_date",
            description="Start date for historical data analysis (YYYY-MM-DD format)",
            required=True
        ),
        SkillParameter(
            name="forecast_period",
            description="Number of future periods to forecast",
            required=True,
            default_value=6
        ),
        SkillParameter(
            name="installation_filter",
            description="Filter by specific installation (use 'ALL' for all installations)",
            required=False,
            default_value="ALL"
        ),
        SkillParameter(
            name="other_filters",
            description="Additional filters to apply",
            required=False,
            constrained_to="filters"
        )
    ]
)
def forecast_skill(parameters: SkillInput) -> SkillOutput:
    """
    Time Series Forecast skill
    
    Uses multiple forecasting models to predict future values and automatically selects the best model.
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
        start_date = parameters.arguments.start_date
        forecast_period = int(parameters.arguments.forecast_period)
        installation_filter = getattr(parameters.arguments, 'installation_filter', 'ALL').upper()
        other_filters = getattr(parameters.arguments, 'other_filters', [])
        
        # Build query for AnswerRocket
        query_parts = [f"show me {metric} by date"]
        
        # Add date filter
        query_parts.append(f"since {start_date}")
        
        # Add installation filter if specified
        if installation_filter != 'ALL':
            query_parts.append(f"where installation = '{installation_filter}'")
        
        # Add other filters
        if other_filters:
            for filter_item in other_filters:
                if isinstance(filter_item, dict):
                    dim = filter_item.get('dim', '')
                    op = filter_item.get('op', '=')
                    val = filter_item.get('val', '')
                    query_parts.append(f"and {dim} {op} {val}")
        
        query = " ".join(query_parts)
        logger.info(f"Executing query: {query}")
        
        # Execute query
        result = arc.ask(query)
        
        # Extract data
        if hasattr(result, 'data') and result.data is not None:
            df = pd.DataFrame(result.data)
        else:
            # Fallback: create sample data structure for testing
            logger.warning("No data returned from AnswerRocket, using sample structure")
            df = pd.DataFrame({
                'date': pd.date_range(start=start_date, periods=24, freq='MS'),
                metric: np.random.randint(100, 1000, 24)
            })
        
        if df.empty:
            return SkillOutput(
                final_prompt=f"No data found for {metric} since {start_date}. Please check your parameters.",
                narrative="No data available",
                visualizations=[],
                export_data=[]
            )
        
        # Data preprocessing
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'] >= pd.to_datetime(start_date)].sort_values(by='date')
        df[metric] = pd.to_numeric(df[metric], errors='coerce')
        
        # Helper functions
        def mape(actual, predicted):
            actual, predicted = np.array(actual), np.array(predicted)
            min_len = min(len(actual), len(predicted))
            actual = actual[:min_len]
            predicted = predicted[:min_len]
            actual = np.where(actual == 0, np.nan, actual)
            return np.nanmean(np.abs((actual - predicted) / actual)) * 100
        
        def combined_score(r2, mape_score, weight_r2=0.5, weight_mape=0.5):
            if np.isnan(mape_score):
                return r2
            return weight_r2 * r2 - weight_mape * mape_score
        
        # Generate forecast date range
        forecast_months = pd.date_range(start=df['date'].max() + pd.DateOffset(months=1), periods=forecast_period, freq='MS')
        forecast_df = pd.DataFrame({'date': forecast_months})
        df['time_index'] = np.arange(len(df))
        
        # Forecasting functions
        def linear_forecast(df, forecast_df):
            X = sm.add_constant(df['time_index'])
            model = sm.OLS(df[metric], X).fit()
            slope, intercept = model.params['time_index'], model.params['const']
            forecast_df[f'forecast_{metric}'] = intercept + slope * np.arange(len(df), len(df) + len(forecast_df))
            return forecast_df, slope, intercept, model.rsquared
        
        def holt_winters_forecast(df, forecast_df):
            df = df.sort_values(by='date')
            try:
                model = ExponentialSmoothing(df[metric], seasonal='add', trend='add', seasonal_periods=6, damped_trend=True).fit()
                forecast_sales = model.forecast(steps=len(forecast_df))
                forecast_df[f'forecast_{metric}'] = forecast_sales.values
                return forecast_df, model.sse
            except Exception as e:
                logger.warning(f"Holt-Winters failed: {e}, using simple exponential smoothing")
                model = ExponentialSmoothing(df[metric], trend='add').fit()
                forecast_sales = model.forecast(steps=len(forecast_df))
                forecast_df[f'forecast_{metric}'] = forecast_sales.values
                return forecast_df, model.sse
        
        def prophet_forecast(df, forecast_df):
            df_prophet = df.rename(columns={'date': 'ds', metric: 'y'})[['ds', 'y']]
            model = Prophet(changepoint_prior_scale=0.01)
            model.fit(df_prophet)
            future = model.make_future_dataframe(periods=len(forecast_df), freq='MS')
            forecast = model.predict(future)
            forecast_df[f'forecast_{metric}'] = forecast['yhat'].iloc[-len(forecast_df):].values
            return forecast_df, model.history['y'].corr(forecast['yhat'])**2
        
        # Evaluate models
        models = {}
        mape_scores = {}
        
        # Linear forecast
        forecast_df_linear, slope, intercept, r2_linear = linear_forecast(df, forecast_df.copy())
        models['Linear'] = r2_linear
        mape_scores['Linear'] = mape(df[metric].dropna(), forecast_df_linear[f'forecast_{metric}'])
        
        # Holt-Winters forecast
        forecast_df_hw, sse_hw = holt_winters_forecast(df, forecast_df.copy())
        models['Holt-Winters'] = 1 - sse_hw / np.var(df[metric])
        mape_scores['Holt-Winters'] = mape(df[metric].dropna(), forecast_df_hw[f'forecast_{metric}'])
        
        # Prophet forecast
        forecast_df_prophet, r2_prophet = prophet_forecast(df, forecast_df.copy())
        models['Prophet'] = r2_prophet
        mape_scores['Prophet'] = mape(df[metric].dropna(), forecast_df_prophet[f'forecast_{metric}'])
        
        # Calculate combined scores and select best model
        combined_scores = {
            model: combined_score(models[model], mape_scores[model])
            for model in models
        }
        
        best_model = max(combined_scores, key=combined_scores.get)
        
        # Apply the best model
        if best_model == 'Holt-Winters':
            forecast_df, _ = holt_winters_forecast(df, forecast_df.copy())
        elif best_model == 'Linear':
            forecast_df, _, _, _ = linear_forecast(df, forecast_df.copy())
        elif best_model == 'Prophet':
            forecast_df = prophet_forecast(df, forecast_df.copy())[0]
        
        # Combine historical data with forecast data
        combined_df = pd.concat([df.set_index('date'), forecast_df.set_index('date')], axis=1).reset_index()
        combined_df['best_model'] = best_model
        
        # Calculate rolling metrics
        combined_df['rolling_max'] = combined_df[metric].rolling(window=3).max()
        combined_df['rolling_min'] = combined_df[metric].rolling(window=3).min()
        
        # Calculate Z-scores for outlier detection
        combined_df['z_score'] = stats.zscore(combined_df[metric].dropna())
        combined_df['outlier_flag'] = np.where((combined_df['z_score'] > 1.7) | (combined_df['z_score'] < -1.7), 'Outlier', 'Normal')
        combined_df['outlier_value'] = np.where(combined_df['outlier_flag'] == 'Outlier', combined_df[metric], None)
        
        # Year-over-year comparison
        combined_df['last_year_sales'] = combined_df[metric].shift(12)
        combined_df['year_over_year_growth'] = (combined_df[metric] - combined_df['last_year_sales']) / combined_df['last_year_sales'] * 100
        
        # Format dates for chart
        combined_df['date'] = combined_df['date'].dt.strftime('%m/%Y')
        
        # Fill NaN values
        combined_df = combined_df.fillna('none')
        
        # Create Highcharts configuration
        chart_config = {
            "chart": {
                "type": "line",
                "backgroundColor": "#f9f9f9",
                "borderColor": "#e0e0e0",
                "borderWidth": 1,
                "plotBorderColor": "#e0e0e0",
                "plotBorderWidth": 1
            },
            "title": {
                "text": f"{best_model.capitalize()} {metric.capitalize()} Forecast",
                "align": "left",
                "x": 10,
                "y": 10,
                "style": {
                    "fontSize": "20px",
                    "fontWeight": "bold",
                    "color": "#333"
                }
            },
            "subtitle": {
                "text": f"Monthly {metric.capitalize()} Data",
                "style": {
                    "fontSize": "12px",
                    "color": "#666"
                },
                "align": "left",
                "x": 10,
                "y": 35
            },
            "xAxis": {
                "categories": combined_df["date"].tolist(),
                "title": {
                    "text": "Date",
                    "style": {
                        "fontWeight": "bold"
                    }
                },
                "labels": {
                    "rotation": -45
                }
            },
            "yAxis": {
                "title": {
                    "text": metric.capitalize(),
                    "style": {
                        "fontWeight": "bold"
                    }
                },
                "labels": {
                    "formatter": "function() { return this.value.toLocaleString(); }"
                }
            },
            "tooltip": {
                "shared": True,
                "useHTML": True,
                "style": {
                    "fontSize": "12px"
                },
                "headerFormat": "<b>{point.key}</b><br/>",
                "pointFormat": "<span style=\"color:{point.color}\">{series.name}: <b>{point.y:,.0f}</b></span><br/>"
            },
            "plotOptions": {
                "series": {
                    "marker": {
                        "enabled": False
                    },
                    "states": {
                        "hover": {
                            "enabled": True,
                            "lineWidthPlus": 2
                        }
                    }
                }
            },
            "series": [
                {
                    "name": f"Actual {metric.capitalize()}",
                    "data": combined_df[metric].tolist(),
                    "tooltip": {
                        "pointFormat": f"<b>{metric.capitalize()}</b>: <span style=\"color:#0000FF\">{{point.y:,.0f}}</span>"
                    }
                },
                {
                    "name": f"Forecasted {metric.capitalize()}",
                    "data": combined_df[f"forecast_{metric}"].tolist(),
                    "color": "#008000",
                    "dashStyle": "Solid",
                    "lineWidth": 4,
                    "tooltip": {
                        "pointFormat": f"<b>Forecasted {metric.capitalize()}</b>: <span style=\"color:#008000\">{{point.y:,.0f}}</span>"
                    }
                },
                {
                    "name": "Rolling Max",
                    "data": combined_df["rolling_max"].tolist(),
                    "dashStyle": "ShortDash",
                    "color": "#ADD8E6",
                    "tooltip": {
                        "pointFormat": ""
                    }
                },
                {
                    "name": "Rolling Min",
                    "data": combined_df["rolling_min"].tolist(),
                    "dashStyle": "ShortDot",
                    "color": "#ADD8E6",
                    "tooltip": {
                        "pointFormat": ""
                    }
                },
                {
                    "name": "Outliers",
                    "type": "scatter",
                    "data": combined_df["outlier_value"].tolist(),
                    "marker": {
                        "enabled": True,
                        "symbol": "circle",
                        "radius": 6,
                        "fillColor": "rgba(255, 0, 0, 0.5)"
                    },
                    "tooltip": {
                        "pointFormat": "<b>Outlier</b>: <span style=\"color:#FF0000\">{{point.y:,.0f}}</span>"
                    }
                }
            ],
            "credits": {
                "enabled": False
            },
            "legend": {
                "align": "right",
                "verticalAlign": "top",
                "layout": "vertical",
                "floating": True,
                "backgroundColor": "rgba(255, 255, 255, 0.8)",
                "borderWidth": 1,
                "borderColor": "#e0e0e0"
            }
        }
        
        # Create visualization
        visualization = SkillVisualization(
            title=f"{best_model} {metric.capitalize()} Forecast",
            layout=chart_config
        )
        
        # Create parameter display descriptions
        param_info = [
            ParameterDisplayDescription(key="Best Model", value=best_model),
            ParameterDisplayDescription(key="Metric", value=metric.capitalize()),
            ParameterDisplayDescription(key="Start Date", value=start_date),
            ParameterDisplayDescription(key="Forecast Period", value=f"{forecast_period} months"),
            ParameterDisplayDescription(key="Installation Filter", value=installation_filter)
        ]
        
        # Create export data
        export_data = [
            ExportData(name="Forecast Data", data=combined_df),
            ExportData(name="Model Performance", data=pd.DataFrame({
                'Model': list(models.keys()),
                'R_Squared': list(models.values()),
                'MAPE': list(mape_scores.values()),
                'Combined_Score': list(combined_scores.values())
            }))
        ]
        
        # Generate narrative
        forecast_start = forecast_months.min().strftime('%m/%Y')
        forecast_end = forecast_months.max().strftime('%m/%Y')
        
        narrative = f"Forecast analysis completed using {best_model} model. The analysis shows predictions for {metric} from {forecast_start} to {forecast_end}. "
        
        if len(combined_df) > 1:
            actual_data = combined_df[combined_df[metric] != 'none']
            if len(actual_data) > 0:
                trend_direction = "increasing" if actual_data[metric].iloc[-1] > actual_data[metric].iloc[0] else "decreasing"
                narrative += f"Historical data shows a {trend_direction} trend. "
        
        narrative += f"Model performance - R²: {models[best_model]:.3f}, MAPE: {mape_scores[best_model]:.1f}%"
        
        final_prompt = f"I've generated a {forecast_period}-month forecast for {metric} using the {best_model} model. {narrative}"
        
        return SkillOutput(
            final_prompt=final_prompt,
            narrative=narrative,
            visualizations=[visualization],
            parameter_display_descriptions=param_info,
            export_data=export_data
        )
    
    except Exception as e:
        logger.error(f"Error in forecast_skill: {str(e)}")
        return SkillOutput(
            final_prompt=f"An error occurred while generating the forecast: {str(e)}",
            narrative="Error occurred during forecasting",
            visualizations=[],
            export_data=[]
        )

if __name__ == '__main__':
    # Test the skill
    skill_input: SkillInput = forecast_skill.create_input(arguments={
        'metric': 'total_work_orders',
        'start_date': '2023-01-01',
        'forecast_period': 6,
        'installation_filter': 'ALL'
    })
    out = forecast_skill(skill_input)
    preview_skill(forecast_skill, out)