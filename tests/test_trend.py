import os
from types import SimpleNamespace

from answer_rocket import AnswerRocketClient
from skill_framework import ParameterDisplayDescription, SkillInput
from ar_analytics.helpers.dataset_context import DatasetContext
from ar_analytics.trend import TrendTemplateParameterSetup, AdvanceTrend, TrendAnalysis
from ar_analytics.helpers.utils import render_layout
from skill_framework.preview import preview_skill


def test_trend():
    '''
    Able to run AdvanceTrend without error
    '''

    parameters = {"metrics": ["sales", "volume"], "breakouts": [], "periods": ["2022"], "growth_type": "Y/Y"}

    default_param_dict = {"periods": [], "metrics": None, "limit_n": 10, "breakouts": [], "growth_type": None, "other_filters": [], "time_granularity": None}
    env = SimpleNamespace(**default_param_dict)

    vars(env).update(parameters)
    TrendTemplateParameterSetup(env=env)
    env.trend = AdvanceTrend.from_env(env=env)
    df = env.trend.run_from_env()
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.trend.paramater_display_infomation.items()]
    tables = [env.trend.display_dfs.get("Metrics Table")]

    insights_dfs = [env.trend.df_notes, env.trend.facts, env.trend.top_facts, env.trend.bottom_facts]

    charts = env.trend.get_dynamic_layout_chart_vars()

    assert True

def test_trend_variance():
    '''
    Able to run VarianceTrend without error
    '''

    parameters = {"metrics": ["sales"], "breakouts": [], "periods": ["2022"], "growth_type": "vs. Budget"}
    # parameters = {"metrics": ["sales"], "breakouts": ["brand"], "periods": ["2022"], "growth_type": "vs. Budget"}      

    default_param_dict = {"periods": [], "metrics": None, "limit_n": 10, "breakouts": [], "growth_type": None, "other_filters": [], "time_granularity": None}
    env = SimpleNamespace(**default_param_dict)

    vars(env).update(parameters)

    TrendTemplateParameterSetup(env=env)
    env.trend = AdvanceTrend.from_env(env=env)
    df = env.trend.run_from_env()
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.trend.paramater_display_infomation.items()]
    tables = [env.trend.display_dfs.get("Metrics Table")]

    insights_dfs = [env.trend.df_notes, env.trend.facts, env.trend.top_facts, env.trend.bottom_facts]

    charts = env.trend.get_dynamic_layout_chart_vars()

    assert True


def test_trend_analysis_directly():
    parameters = {"metrics": ["sales", "volume"], "breakouts": [], "periods": ["2022"], "growth_type": None}  # No growth type
    
    default_param_dict = {"periods": [], "metrics": None, "limit_n": 10, "breakouts": [], "growth_type": None, "other_filters": [], "time_granularity": None}
    env = SimpleNamespace(**default_param_dict)
    
    vars(env).update(parameters)
    TrendTemplateParameterSetup(env=env)
    env.trend = TrendAnalysis.from_env(env=env)
    df = env.trend.run_from_env()
    
    charts = env.trend.get_charts(df, parameters["metrics"], parameters["breakouts"])
    print(f"charts: {charts}")

    assert True