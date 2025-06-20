from dataclasses import dataclass
from typing import Dict
from dimension_breakout import simple_breakout
from skill_framework import ExitFromSkillException, SkillInput
from skill_framework.preview import preview_skill
from dataset_definitions.pasta_v9 import PastaV9TestColumnNames

@dataclass
class TestBreakoutCommonParametersConfig:
    metric_1: str
    metric_2: str
    share_metric_1: str
    share_metric_2: str
    breakout_1: str
    breakout_2: str
    period_filter: str
    filter_1: dict
    filter_2: dict
    growth_type__yoy: str = "Y/Y"
    growth_type__pop: str = "P/P"
    growth_trend__fastest_growing: str = "fastest growing"
    growth_trend__highest_growing: str = "highest growing"
    growth_trend__highest_declining: str = "highest declining"
    growth_trend__fastest_declining: str = "fastest declining"
    growth_trend__smallest_overall: str = "smallest overall"
    growth_trend__biggest_overall: str = "biggest overall"

PastaV9BreakoutCommonParametersConfig = TestBreakoutCommonParametersConfig(
    metric_1=PastaV9TestColumnNames.SALES.value,
    metric_2=PastaV9TestColumnNames.VOLUME.value,
    share_metric_1=PastaV9TestColumnNames.SALES_SHARE.value,
    share_metric_2=PastaV9TestColumnNames.VOLUME_SHARE.value,
    breakout_1=PastaV9TestColumnNames.BRAND.value,
    breakout_2=PastaV9TestColumnNames.MANUFACTURER.value,
    period_filter="2022",
    filter_1={"dim": PastaV9TestColumnNames.SUB_CATEGORY.value, "op": "=", "val": PastaV9TestColumnNames.SUB_CATEGORY__SEMOLINA.value},
    filter_2={"dim": PastaV9TestColumnNames.MANUFACTURER.value, "op": "=", "val": PastaV9TestColumnNames.MANUFACTURER__PRIVATE_LABEL.value}
)

class TestBreakout:

    def _run_breakout(self, parameters: Dict, preview: bool = False):
        skill_input: SkillInput = simple_breakout.create_input(arguments=parameters)
        out = simple_breakout(skill_input)
        if preview or self.preview:
            preview_skill(simple_breakout, out)
        return out

    def _assert_breakout_runs_with_error(self, parameters: Dict, expected_exception: Exception):
        try:
            self._run_breakout(parameters, preview=False)
        except Exception as e:
            assert isinstance(e, expected_exception)

    def _assert_breakout_runs_without_errors(self, parameters: Dict, preview: bool = False):
        self._run_breakout(parameters, preview=preview)
        assert True

class TestLegacyBreakout(TestBreakout):
    '''
    Test the simple_breakout skill with common parameters to see if it runs without errors or raises an error
    '''

    config = PastaV9BreakoutCommonParametersConfig
    preview = False

    def test_simple_breakout_skill(self):
        '''
        Checks to see if the simple breakout skill runs without errors
        '''
        # currently assumes it's attached to the pasta dataset
        skill_input: SkillInput = simple_breakout.create_input(arguments={'metrics': ["sales", "volume"], 'breakouts': ["brand", "manufacturer"], 'periods': ["2022"], 'growth_type': "Y/Y", 'other_filters': []})
        out = simple_breakout(skill_input)
        preview_skill(simple_breakout, out)
        assert True

class TestBreakoutCommonParameters(TestBreakout):
    '''
    Test the breakout skill with common parameters to see if it runs without errors or raises an error
    '''

    config = PastaV9BreakoutCommonParametersConfig
    preview = False

    def test_single_metric_single_breakout(self):
        """Test with a single metric and single breakout"""
        parameters = {
            "metrics": [self.config.metric_1],
            "breakouts": [self.config.breakout_1]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_multiple_breakouts(self):
        """Test with a single metric and multiple breakouts"""
        parameters = {
            "metrics": [self.config.metric_1],
            "breakouts": [self.config.breakout_1, self.config.breakout_2]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_multiple_metrics_single_breakout(self):
        """Test with multiple metrics and single breakout"""
        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "breakouts": [self.config.breakout_1]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_multiple_metrics_multiple_breakouts(self):
        """Test with multiple metrics and multiple breakouts"""
        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "breakouts": [self.config.breakout_1, self.config.breakout_2]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout(self):
        """Test with a single metric, period, and breakout"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_period_and_growth_type(self):
        """Test with a single metric, period, and growth type"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_period_and_pop_growth_type(self):
        """Test with a single metric, period, and P/P growth type"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__pop
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_period_and_filter(self):
        """Test with a single metric, period, breakout, and filter"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_period_and_multiple_filters(self):
        """Test with a single metric, period, breakout, and multiple filters"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "other_filters": [self.config.filter_1, self.config.filter_2]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_limit_n(self):
        """Test with a single metric, breakout, and limit_n"""
        parameters = {
            "metrics": [self.config.metric_1],
            "breakouts": [self.config.breakout_1],
            "limit_n": 5
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_fastest_growing(self):
        """Test with a single metric, breakout, and fastest growing trend"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy,
            "growth_trend": self.config.growth_trend__fastest_growing
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_highest_growing(self):
        """Test with a single metric, breakout, and highest growing trend"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy,
            "growth_trend": self.config.growth_trend__highest_growing
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_highest_declining(self):
        """Test with a single metric, breakout, and highest declining trend"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy,
            "growth_trend": self.config.growth_trend__highest_declining
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_fastest_declining(self):
        """Test with a single metric, breakout, and fastest declining trend"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy,
            "growth_trend": self.config.growth_trend__fastest_declining
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_smallest_overall(self):
        """Test with a single metric, breakout, and smallest overall trend"""
        parameters = {
            "metrics": [self.config.metric_1],
            "breakouts": [self.config.breakout_1],
            "growth_trend": self.config.growth_trend__smallest_overall
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_biggest_overall(self):
        """Test with a single metric, breakout, and biggest overall trend"""
        parameters = {
            "metrics": [self.config.metric_1],
            "breakouts": [self.config.breakout_1],
            "growth_trend": self.config.growth_trend__biggest_overall
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_calculated_metric_filters_growth(self):
        """Test with calculated metric filters for growth"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy,
            "calculated_metric_filters": [
                {
                    "metric": self.config.metric_1,
                    "computation": "growth",
                    "operator": ">",
                    "value": 0.05,
                    "scale": "percentage"
                }
            ]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_calculated_metric_filters_share(self):
        """Test with calculated metric filters for share"""
        parameters = {
            "metrics": [self.config.share_metric_1],
            "breakouts": [self.config.breakout_1],
            "calculated_metric_filters": [
                {
                    "metric": self.config.share_metric_1,
                    "computation": "share",
                    "operator": ">=",
                    "value": 0.10,
                    "scale": "percentage"
                }
            ]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_calculated_metric_filters_delta(self):
        """Test with calculated metric filters for delta"""
        parameters = {
            "metrics": [self.config.metric_1],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "calculated_metric_filters": [
                {
                    "metric": self.config.metric_1,
                    "computation": "delta",
                    "operator": "between",
                    "value": [1000, 10000],
                    "scale": "absolute"
                }
            ]
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_full_parameter_combination(self):
        """Test with all parameters combined"""
        parameters = {
            "metrics": [self.config.metric_1, self.config.metric_2],
            "periods": [self.config.period_filter],
            "breakouts": [self.config.breakout_1],
            "growth_type": self.config.growth_type__yoy,
            "other_filters": [self.config.filter_1],
            "growth_trend": self.config.growth_trend__fastest_growing,
            "limit_n": 8
        }
        self._assert_breakout_runs_without_errors(parameters)

    def test_share_metrics_with_breakouts(self):
        """Test with share metrics and breakouts"""
        parameters = {
            "metrics": [self.config.share_metric_1, self.config.share_metric_2],
            "breakouts": [self.config.breakout_1, self.config.breakout_2],
            "periods": [self.config.period_filter]
        }
        self._assert_breakout_runs_without_errors(parameters)