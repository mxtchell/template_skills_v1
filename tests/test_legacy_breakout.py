from dataclasses import dataclass
from typing import Dict, List
from enum import Enum
from dimension_breakout import simple_breakout  # Assuming the file is named simple_breakout.py
from skill_framework import ExitFromSkillException, SkillInput
from skill_framework.preview import preview_skill
from dataset_definitions.pasta_v9 import PastaV9TestColumnNames


class LegacyBreakoutParameters(Enum):
    """Parameter names for legacy breakout skill with clean reference system"""
    metrics = "metrics"
    breakouts = "breakouts"
    periods = "periods"
    limit_n = "limit_n"
    growth_type = "growth_type"
    growth_trend = "growth_trend"
    other_filters = "other_filters"
    calculated_metric_filters = "calculated_metric_filters"
    max_prompt = "max_prompt"
    insight_prompt = "insight_prompt"
    table_viz_layout = "table_viz_layout"
    bridge_chart_viz_layout = "bridge_chart_viz_layout"


@dataclass
class LegacyBreakoutCommonParametersConfig:
    """Configuration for common parameter testing using pasta_v9 dataset"""
    metric_1: str
    metric_2: str
    share_metric_1: str
    share_metric_2: str
    breakout_1: str
    breakout_2: str
    period_filter: str
    filter_1: dict
    filter_2: dict
    growth_type_yoy: str = "Y/Y"
    growth_type_pop: str = "P/P"
    growth_type_none: str = "None"
    growth_trend_fastest_growing: str = "fastest growing"
    growth_trend_highest_growing: str = "highest growing"
    growth_trend_highest_declining: str = "highest declining"
    growth_trend_fastest_declining: str = "fastest declining"
    growth_trend_smallest_overall: str = "smallest overall"
    growth_trend_biggest_overall: str = "biggest overall"


PastaV9LegacyBreakoutCommonParametersConfig = LegacyBreakoutCommonParametersConfig(
    metric_1=PastaV9TestColumnNames.SALES.value,
    metric_2=PastaV9TestColumnNames.ACV.value,
    share_metric_1=PastaV9TestColumnNames.SALES_SHARE.value,
    share_metric_2=PastaV9TestColumnNames.VOLUME_SHARE.value,
    breakout_1=PastaV9TestColumnNames.BRAND.value,
    breakout_2=PastaV9TestColumnNames.BASE_SIZE.value,
    period_filter="2022",
    filter_1={"dim": PastaV9TestColumnNames.SUB_CATEGORY.value, "op": "=", "val": PastaV9TestColumnNames.SUB_CATEGORY__SEMOLINA.value},
    filter_2={"dim": PastaV9TestColumnNames.MANUFACTURER.value, "op": "=", "val": PastaV9TestColumnNames.MANUFACTURER__PRIVATE_LABEL.value}
)


@dataclass
class LegacyBreakoutGuardrailsConfig:
    """Configuration for testing guardrails and edge cases"""
    invalid_metric: str = "invalid_metric"
    invalid_metric_from_pasta: str = PastaV9TestColumnNames.ACV_SHARE.value  # Not a valid metric for legacy breakout
    invalid_breakout: str = "invalid_breakout"
    too_many_breakouts: List[str] = None
    invalid_growth_type: str = "invalid_growth"
    invalid_growth_trend: str = "invalid_trend"
    empty_metrics: List[str] = None
    
    def __post_init__(self):
        if self.too_many_breakouts is None:
            # Assuming more than 3-4 breakouts might be too many
            self.too_many_breakouts = [
                PastaV9TestColumnNames.BRAND.value,
                PastaV9TestColumnNames.BASE_SIZE.value,
                PastaV9TestColumnNames.MANUFACTURER.value,
                PastaV9TestColumnNames.SUB_CATEGORY.value,
                "excessive_breakout"
            ]
        if self.empty_metrics is None:
            self.empty_metrics = []


PastaV9LegacyBreakoutGuardrailsConfig = LegacyBreakoutGuardrailsConfig()


class TestLegacyBreakout:
    """Base test class with helper methods for legacy breakout testing"""

    def _run_legacy_breakout(self, parameters: Dict, preview: bool = False):
        skill_input: SkillInput = simple_breakout.create_input(arguments=parameters)
        out = simple_breakout(skill_input)
        if preview or getattr(self, 'preview', False):
            preview_skill(simple_breakout, out)
        return out

    def _assert_legacy_breakout_runs_with_error(self, parameters: Dict, expected_exception: Exception):
        try:
            self._run_legacy_breakout(parameters, preview=False)
            assert False, f"Expected {expected_exception.__name__} but skill ran successfully"
        except Exception as e:
            assert isinstance(e, expected_exception), f"Expected {expected_exception.__name__}, got {type(e).__name__}"

    def _assert_legacy_breakout_runs_without_errors(self, parameters: Dict, preview: bool = False):
        self._run_legacy_breakout(parameters, preview=preview)
        assert True


class TestLegacyBreakoutCommonParameters(TestLegacyBreakout):
    """Test the legacy breakout skill with common parameters to verify functionality"""

    config = PastaV9LegacyBreakoutCommonParametersConfig
    preview = False

    def test_single_metric_no_breakout(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_breakout(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_type_yoy(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_yoy
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_type_pop(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_pop
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_fastest_growing(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_yoy,
            LegacyBreakoutParameters.growth_trend.value: self.config.growth_trend_fastest_growing
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_trend_highest_declining(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_yoy,
            LegacyBreakoutParameters.growth_trend.value: self.config.growth_trend_highest_declining
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_filter(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_limit_n(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.limit_n.value: 5
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_single_metric_with_growth_type_none(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_none
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_growth_trend_biggest_overall(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.growth_trend.value: self.config.growth_trend_biggest_overall
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_growth_trend_smallest_overall(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.growth_trend.value: self.config.growth_trend_smallest_overall
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_all_valid_raw_metrics(self):
        valid_raw_metrics = ["sales", "acv", "volume"]
        parameters = {
            LegacyBreakoutParameters.metrics.value: valid_raw_metrics,
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_all_valid_share_metrics(self):
        valid_share_metrics = ["sales_share", "volume_share"]
        parameters = {
            LegacyBreakoutParameters.metrics.value: valid_share_metrics,
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_multiple_metrics_with_breakout(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1, self.config.metric_2],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_multiple_metrics_with_multiple_breakouts(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1, self.config.metric_2],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1, self.config.breakout_2]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_share_metrics_with_breakout(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.share_metric_1, self.config.share_metric_2],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_complex_parameter_combination(self):
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1, self.config.metric_2],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1, self.config.breakout_2],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_yoy,
            LegacyBreakoutParameters.growth_trend.value: self.config.growth_trend_fastest_growing,
            LegacyBreakoutParameters.other_filters.value: [self.config.filter_1],
            LegacyBreakoutParameters.limit_n.value: 15
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)

    def test_calculated_metric_filters_basic(self):
        calculated_filters = [
            {
                "metric": self.config.metric_1,
                "computation": "growth",
                "operator": ">",
                "value": 0,
                "scale": "percentage"
            }
        ]
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_yoy,
            LegacyBreakoutParameters.calculated_metric_filters.value: calculated_filters
        }
        self._assert_legacy_breakout_runs_without_errors(parameters)


class TestLegacyBreakoutGuardrails(TestLegacyBreakout):
    """Test guardrails and error conditions for legacy breakout skill"""

    config = PastaV9LegacyBreakoutCommonParametersConfig
    guardrail_config = PastaV9LegacyBreakoutGuardrailsConfig
    preview = False

    def test_no_metrics_provided(self):
        """Test that skill fails when no metrics are provided"""
        parameters = {
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_empty_metrics_list(self):
        """Test that skill fails when empty metrics list is provided"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: self.guardrail_config.empty_metrics,
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_invalid_metric_completely_unknown(self):
        """Test that skill fails with completely unknown metric"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.guardrail_config.invalid_metric],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_invalid_metric_from_pasta_dataset(self):
        """Test that skill fails with acv_share (exists in pasta but not supported by legacy breakout)"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.guardrail_config.invalid_metric_from_pasta],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_invalid_growth_type(self):
        """Test that skill fails with invalid growth type"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.growth_type.value: self.guardrail_config.invalid_growth_type
        }
        self._assert_legacy_breakout_runs_with_error(parameters, (ExitFromSkillException, ValueError))

    def test_invalid_growth_trend(self):
        """Test that skill fails with invalid growth trend"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.growth_trend.value: self.guardrail_config.invalid_growth_trend
        }
        self._assert_legacy_breakout_runs_with_error(parameters, (ExitFromSkillException, ValueError))

    def test_growth_type_without_periods(self):
        """Test that growth_type without periods fails appropriately"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.growth_type.value: self.config.growth_type_yoy
            # Intentionally missing periods
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_growth_trend_without_growth_type(self):
        """Test that skill handles growth_trend without growth_type appropriately"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.periods.value: [self.config.period_filter],  # Added periods!
            LegacyBreakoutParameters.growth_trend.value: self.config.growth_trend_fastest_growing
            # No growth_type specified
        }
        # This might be valid or invalid - need to test what the actual behavior is
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_calculated_metric_filters_invalid_metric(self):
        """Test that calculated metric filters fail with invalid metric"""
        calculated_filters = [
            {
                "metric": self.guardrail_config.invalid_metric,
                "computation": "growth",
                "operator": ">",
                "value": 0,
                "scale": "percentage"
            }
        ]
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.calculated_metric_filters.value: calculated_filters
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_calculated_metric_filters_invalid_computation(self):
        """Test that calculated metric filters fail with invalid computation"""
        calculated_filters = [
            {
                "metric": self.config.metric_1,
                "computation": "invalid_computation",
                "operator": ">",
                "value": 0,
                "scale": "percentage"
            }
        ]
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.calculated_metric_filters.value: calculated_filters
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_excessive_breakouts(self):
        """Test that skill fails or handles excessive number of breakouts"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: self.guardrail_config.too_many_breakouts
        }
        # This might succeed or fail depending on the implementation
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_negative_limit_n(self):
        """Test that skill handles negative limit_n appropriately"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.limit_n.value: -5
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)

    def test_zero_limit_n(self):
        """Test that skill handles zero limit_n appropriately"""
        parameters = {
            LegacyBreakoutParameters.metrics.value: [self.config.metric_1],
            LegacyBreakoutParameters.breakouts.value: [self.config.breakout_1],
            LegacyBreakoutParameters.limit_n.value: 0
        }
        self._assert_legacy_breakout_runs_with_error(parameters, ExitFromSkillException)