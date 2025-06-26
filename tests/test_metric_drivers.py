from dataclasses import dataclass
from typing import Dict, List
from enum import Enum
from metric_drivers import simple_metric_driver
from skill_framework import ExitFromSkillException, SkillInput
from skill_framework.preview import preview_skill
from dataset_definitions.pasta_v9 import PastaV9TestColumnNames


class MetricDriversParameters(Enum):
    """Parameter names for metric drivers skill with clean reference system"""
    metric = "metric"
    breakouts = "breakouts"
    periods = "periods"
    limit_n = "limit_n"
    growth_type = "growth_type"
    other_filters = "other_filters"
    calculated_metric_filters = "calculated_metric_filters"
    max_prompt = "max_prompt"
    insight_prompt = "insight_prompt"
    table_viz_layout = "table_viz_layout"


@dataclass
class MetricDriversCommonParametersConfig:
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


PastaV9MetricDriversCommonParametersConfig = MetricDriversCommonParametersConfig(
    metric_1=PastaV9TestColumnNames.SALES.value,
    metric_2=PastaV9TestColumnNames.ACV.value,
    share_metric_1=PastaV9TestColumnNames.SALES_SHARE.value,
    share_metric_2=PastaV9TestColumnNames.VOLUME_SHARE.value,
    breakout_1=PastaV9TestColumnNames.BRAND.value,
    breakout_2=PastaV9TestColumnNames.BASE_SIZE.value,
    period_filter="2023",
    filter_1={"dim": PastaV9TestColumnNames.SUB_CATEGORY.value, "op": "=", "val": PastaV9TestColumnNames.SUB_CATEGORY__SEMOLINA.value},
    filter_2={"dim": PastaV9TestColumnNames.MANUFACTURER.value, "op": "=", "val": PastaV9TestColumnNames.MANUFACTURER__PRIVATE_LABEL.value}
)


@dataclass
class MetricDriversGuardrailsConfig:
    """Configuration for testing guardrails and edge cases"""
    invalid_metric: str = "invalid_metric"
    invalid_breakout: str = "invalid_breakout"
    too_many_breakouts: List[str] = None
    invalid_growth_type: str = "invalid_growth"
    empty_metric: str = ""
    
    def __post_init__(self):
        if self.too_many_breakouts is None:
            self.too_many_breakouts = [
                PastaV9TestColumnNames.BRAND.value,
                PastaV9TestColumnNames.BASE_SIZE.value,
                PastaV9TestColumnNames.MANUFACTURER.value,
                PastaV9TestColumnNames.SUB_CATEGORY.value,
                "excessive_breakout"
            ]


PastaV9MetricDriversGuardrailsConfig = MetricDriversGuardrailsConfig()


class TestMetricDrivers:
    """Base test class with helper methods for metric drivers testing"""

    def _run_metric_drivers(self, parameters: Dict, preview: bool = False):
        skill_input: SkillInput = simple_metric_driver.create_input(arguments=parameters)
        out = simple_metric_driver(skill_input)
        if preview or getattr(self, 'preview', False):
            preview_skill(simple_metric_driver, out)
        return out

    def _assert_metric_drivers_runs_with_error(self, parameters: Dict, expected_exception):
        try:
            self._run_metric_drivers(parameters, preview=False)
            assert False, f"Expected exception but skill ran successfully"
        except expected_exception as e:
            pass
        except Exception as e:
            assert False, f"Expected {expected_exception}, got {type(e).__name__}: {e}"

    def _assert_metric_drivers_runs_without_errors(self, parameters: Dict, preview: bool = False):
        self._run_metric_drivers(parameters, preview=preview)
        assert True


class TestMetricDriversCommonParameters(TestMetricDrivers):
    """Test the metric drivers skill with common parameters to verify functionality"""

    config = PastaV9MetricDriversCommonParametersConfig
    preview = False

    def test_single_metric_with_period(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter]
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_default_growth_type(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.growth_type.value: self.config.growth_type_yoy
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_pop_growth_type(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.growth_type.value: self.config.growth_type_pop
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_multiple_breakouts(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1, self.config.breakout_2]
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout_and_filter(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1],
            MetricDriversParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout_and_multiple_filters(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1],
            MetricDriversParameters.other_filters.value: [self.config.filter_1, self.config.filter_2]
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_limit_n(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1],
            MetricDriversParameters.limit_n.value: 5
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_complex_parameter_combination(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1, self.config.breakout_2],
            MetricDriversParameters.growth_type.value: self.config.growth_type_yoy,
            MetricDriversParameters.other_filters.value: [self.config.filter_1],
            MetricDriversParameters.limit_n.value: 15
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

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
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1],
            MetricDriversParameters.growth_type.value: self.config.growth_type_yoy,
            MetricDriversParameters.calculated_metric_filters.value: calculated_filters
        }
        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_multiple_periods(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: ["2022", "2023"],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_without_errors(parameters)


class TestMetricDriversGuardrails(TestMetricDrivers):
    """Test guardrails and error conditions for metric drivers skill"""

    config = PastaV9MetricDriversCommonParametersConfig
    guardrail_config = PastaV9MetricDriversGuardrailsConfig
    preview = False

    def test_no_metric_provided(self):
        parameters = {
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_empty_metric(self):
        parameters = {
            MetricDriversParameters.metric.value: self.guardrail_config.empty_metric,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_invalid_metric(self):
        parameters = {
            MetricDriversParameters.metric.value: self.guardrail_config.invalid_metric,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_no_periods_provided(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_share_metrics_not_supported(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.share_metric_1,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_null_metric(self):
        parameters = {
            MetricDriversParameters.metric.value: None,
            MetricDriversParameters.periods.value: [self.config.period_filter],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_empty_periods_list(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: [],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_invalid_periods_format(self):
        parameters = {
            MetricDriversParameters.metric.value: self.config.metric_1,
            MetricDriversParameters.periods.value: ["invalid_date", "bad_period"],
            MetricDriversParameters.breakouts.value: [self.config.breakout_1]
        }
        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

