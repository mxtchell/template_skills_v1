from dataclasses import dataclass
from typing import Dict
from trend import trend
from skill_framework import ExitFromSkillException, SkillInput
from skill_framework.preview import preview_skill
from dataset_definitions.pasta_v9 import PastaV9TestColumnNames

@dataclass
class TestTrendCommonParametersConfig:
    metric_1: str
    metric_2: str
    share_metric_1: str
    share_metric_2: str
    breakout_1: str
    breakout_2: str
    period_filter: str
    filter_1: dict
    filter_2: dict
    time_granularity_1: str = "month"
    time_granularity_2: str = "quarter"
    growth_type__yoy: str = "Y/Y"
    growth_type__pop: str = "P/P"

PastaV9TrendCommonParametersConfig = TestTrendCommonParametersConfig(
    metric_1=PastaV9TestColumnNames.SALES.value,
    metric_2=PastaV9TestColumnNames.ACV.value,
    share_metric_1=PastaV9TestColumnNames.SALES_SHARE.value,
    share_metric_2=PastaV9TestColumnNames.ACV_SHARE.value,
    breakout_1=PastaV9TestColumnNames.BRAND.value,
    breakout_2=PastaV9TestColumnNames.BASE_SIZE.value,
    period_filter="2022",
    filter_1={"dim": PastaV9TestColumnNames.SUB_CATEGORY.value, "op": "=", "val": PastaV9TestColumnNames.SUB_CATEGORY__SEMOLINA.value},
    filter_2={"dim": PastaV9TestColumnNames.MANUFACTURER.value, "op": "=", "val": PastaV9TestColumnNames.MANUFACTURER__PRIVATE_LABEL.value}
) 

class TestTrend:

    def _run_trend(self, parameters: Dict, preview: bool = False):

        skill_input: SkillInput = trend.create_input(arguments=parameters)
        out = trend(skill_input)
        if preview or self.preview:
            preview_skill(trend, out)

        return out

    def _assert_trend_runs_with_error(self, parameters: Dict, expected_exception: Exception):

        try:
            self._run_trend(parameters, preview=False)
        except Exception as e:
            assert isinstance(e, expected_exception)

    def _assert_trend_runs_without_errors(self, parameters: Dict, preview: bool = False):
        
        self._run_trend(parameters, preview=preview)

        assert True

class TestTrendCommonParameters(TestTrend):

    '''
    Test the trend skill with common parameters to see if it runs without errors or raises an error
    '''

    config = PastaV9TrendCommonParametersConfig
    preview = False

    def test_single_metric(self):
        """Test with a single metric, no growth type, no breakout"""

        parameters = {
            "metrics": [self.config.metric_1]
        }   

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_yoy_growth_type(self):
        """Test with a single metric, growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__yoy
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_pop_growth_type(self):
        """Test with a single metric, growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__pop
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_yoy_growth_type(self):
        """Test with a single metric, growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__yoy
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_pop_growth_type(self):
        """Test with a single metric, growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__pop
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout(self):
        """Test with a single metric, breakout"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_filter(self):
        """Test with a single metric, period, and filter"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "other_filters": [self.config.filter_1]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout_and_filter(self):
        """Test with a single metric, period, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1]
        }

        self._assert_trend_runs_without_errors(parameters)  
        
    def test_single_metric_with_period_and_breakout_and_filter_and_filter2(self):
        """Test with a single metric, period, breakout, filter, and filter2"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1, self.config.filter_2]
        }

        self._assert_trend_runs_without_errors(parameters)
        
    def test_single_metric_with_period_and_breakout_and_filter_and_filter2_and_growth_type(self):
        """Test with a single metric, period, breakout, filter, filter2, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1, self.config.filter_2],
            "growth_type": self.config.growth_type__yoy
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics(self):
        """Test with multiple metrics, no growth type, no breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period(self):
        """Test with multiple metrics, period, no growth type, no breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_growth_type(self):
        """Test with multiple metrics, period, growth type, no breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__yoy
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_breakout(self):
        """Test with multiple metrics, period, breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_breakout_and_growth_type(self):
        """Test with multiple metrics, period, breakout, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_breakout_and_filter(self):
        """Test with multiple metrics, period, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_breakout_and_filter_and_filter2(self):
        """Test with multiple metrics, period, breakout, filter, and filter2"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1, self.config.filter_2]
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_breakout_and_filter_and_filter2_and_growth_type(self):
        """Test with multiple metrics, period, breakout, filter, filter2, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1, self.config.filter_2],
            "growth_type": self.config.growth_type__yoy
        }

        self._assert_trend_runs_without_errors(parameters)


@dataclass
class TestTrendVarianceConfig:
    metric_1: str
    metric_2: str
    non_variance_metric: str
    breakout_1: str
    breakout_2: str
    period_filter: str
    filter_1: dict
    filter_2: dict
    time_granularity_1: str = "month"
    time_granularity_2: str = "quarter"
    growth_type__target: str = "vs. Target"
    growth_type__budget: str = "vs. Budget"
    growth_type__forecast: str = "vs. Forecast"

PastaV9TrendVarianceConfig = TestTrendVarianceConfig(
    metric_1=PastaV9TestColumnNames.SALES.value,
    metric_2=PastaV9TestColumnNames.VOLUME.value,
    non_variance_metric=PastaV9TestColumnNames.ACV.value,
    breakout_1=PastaV9TestColumnNames.BRAND.value,
    breakout_2=PastaV9TestColumnNames.BASE_SIZE.value,
    period_filter="2022",
    filter_1={"dim": PastaV9TestColumnNames.SUB_CATEGORY.value, "op": "=", "val": PastaV9TestColumnNames.SUB_CATEGORY__SEMOLINA.value},
    filter_2={"dim": PastaV9TestColumnNames.MANUFACTURER.value, "op": "=", "val": PastaV9TestColumnNames.MANUFACTURER__PRIVATE_LABEL.value}
) 


class TestTrendVariance(TestTrend):

    '''
    Requires changing the growth type parameter constraints to constrained_values=["Y/Y", "P/P", "None", "vs. Budget", "vs. Forecast", "vs. Target"]
    and setting up a pasta v9 dataset with the following variance metrics: Sales, Volume
    '''

    config: TestTrendVarianceConfig = PastaV9TrendVarianceConfig
    preview = True

    def test_non_variance_metric_with_budget(self):
        """Test with a non-variance metric, budget"""

        parameters = {
            "metrics": [self.config.non_variance_metric],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_with_error(parameters, ExitFromSkillException)

    def test_single_metric_with_budget(self):
        """Test with a single metric, budget"""

        parameters = {
            "metrics": [self.config.metric_1],
            "growth_type": self.config.growth_type__budget
        }   

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_target(self):
        """Test with a single metric, target"""

        parameters = {
            "metrics": [self.config.metric_1],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_forecast(self):
        """Test with a single metric, forecast"""

        parameters = {
            "metrics": [self.config.metric_1],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_budget(self):
        """Test with a single metric, period, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_target(self):
        """Test with a single metric, period, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_forecast(self):
        """Test with a single metric, period, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_budget_and_breakout(self):
        """Test with a single metric, period, budget, and breakout"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_target_and_breakout(self):
        """Test with a single metric, period, target, and breakout"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)
        
    def test_single_metric_with_period_and_forecast_and_breakout(self):
        """Test with a single metric, period, forecast, and breakout"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_budget_and_breakout_and_filter(self):
        """Test with a single metric, period, budget, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_target_and_breakout_and_filter(self):
        """Test with a single metric, period, target, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_single_metric_with_period_and_forecast_and_breakout_and_filter(self):
        """Test with a single metric, period, forecast, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    
    def test_multiple_metrics_with_budget(self):
        """Test with multiple metrics, budget"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "growth_type": self.config.growth_type__budget
        }   

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_target(self):
        """Test with multiple metrics, target"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_forecast(self):
        """Test with multiple metrics, forecast"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_budget(self):
        """Test with multiple metrics, period, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_target(self):
        """Test with multiple metrics, period, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_forecast(self):
        """Test with multiple metrics, period, and growth type"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_budget_and_breakout(self):
        """Test with multiple metrics, period, budget, and breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_target_and_breakout(self):
        """Test with multiple metrics, period, target, and breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)
        
    def test_multiple_metrics_with_period_and_forecast_and_breakout(self):
        """Test with multiple metrics, period, forecast, and breakout"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_budget_and_breakout_and_filter(self):
        """Test with multiple metrics, period, budget, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1],
            "growth_type": self.config.growth_type__budget
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_target_and_breakout_and_filter(self):
        """Test with multiple metrics, period, target, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1],
            "growth_type": self.config.growth_type__target
        }

        self._assert_trend_runs_without_errors(parameters)

    def test_multiple_metrics_with_period_and_forecast_and_breakout_and_filter(self):
        """Test with multiple metrics, period, forecast, breakout, and filter"""

        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1],
            "growth_type": self.config.growth_type__forecast
        }

        self._assert_trend_runs_without_errors(parameters)
