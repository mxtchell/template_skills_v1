from dataclasses import dataclass
from typing import Dict
from enum import Enum
from market_share_analysis import market_share_analysis
from skill_framework import ExitFromSkillException, SkillInput
from skill_framework.preview import preview_skill
from dataset_definitions.pasta_v9 import PastaV9TestColumnNames


class MarketShareAnalysisParameters(Enum):
    """
    Parameter names for market share analysis skill with clean reference system
    
    Required parameters: metric, periods, other_filters (subject filter)
    Subject filter must identify the market participant (brand/manufacturer) whose share to analyze
    """
    metric = "metric"
    growth_type = "growth_type"
    other_filters = "other_filters"
    limit_n = "limit_n"
    periods = "periods"
    global_view = "global_view"
    market_view = "market_view"
    include_drivers = "include_drivers"
    market_cols = "market_cols"
    impact_calcs = "impact_calcs"
    decomposition_display_config = "decomposition_display_config"
    subject_metric_config = "subject_metric_config"
    max_prompt = "max_prompt"
    insight_prompt = "insight_prompt"
    table_viz_layout = "table_viz_layout"


@dataclass
class MarketShareAnalysisCommonParametersConfig:
    """Configuration for common parameter testing using pasta_v9 dataset"""
    metric_1: str
    metric_2: str
    share_metric_1: str
    share_metric_2: str
    period_filter: str
    filter_1: dict
    filter_2: dict
    growth_type_yoy: str = "Y/Y"
    growth_type_pop: str = "P/P"
    valid_global_view: str = '[{"dim": "brand", "type": "share"}]'
    valid_market_view: str = '[{"dim": "manufacturer", "type": "share"}]'
    valid_market_cols: str = '["sales", "volume"]'
    valid_impact_calcs: str = '{"method": "contribution"}'
    valid_decomposition_display_config: str = '{"Performance": {"sales": ["current", "prior", "diff"]}}'
    valid_subject_metric_config: str = '{"Performance": {"sales": ["current", "prior", "diff"]}}'


PastaV9MarketShareAnalysisCommonParametersConfig = MarketShareAnalysisCommonParametersConfig(
    metric_1=PastaV9TestColumnNames.SALES.value,
    metric_2=PastaV9TestColumnNames.ACV.value,
    share_metric_1=PastaV9TestColumnNames.SALES_SHARE.value,
    share_metric_2=PastaV9TestColumnNames.VOLUME_SHARE.value,
    period_filter="2022",
    filter_1={"dim": PastaV9TestColumnNames.BRAND.value, "op": "=", "val": "buitoni"},
    filter_2={"dim": PastaV9TestColumnNames.MANUFACTURER.value, "op": "=", "val": [PastaV9TestColumnNames.MANUFACTURER__PRIVATE_LABEL.value]}
)


@dataclass
class MarketShareAnalysisGuardrailsConfig:
    """Configuration for testing guardrails and edge cases"""
    invalid_metric: str = "invalid_metric"
    invalid_growth_type: str = "invalid_growth"
    empty_metric: str = ""
    malformed_json: str = '{"invalid": json}'
    invalid_json_syntax: str = '{"missing_quote: "value"}'
    


PastaV9MarketShareAnalysisGuardrailsConfig = MarketShareAnalysisGuardrailsConfig()


class TestMarketShareAnalysis:
    """Base test class with helper methods for market share analysis testing"""

    def _run_market_share_analysis(self, parameters: Dict, preview: bool = False):
        skill_input: SkillInput = market_share_analysis.create_input(arguments=parameters)
        out = market_share_analysis(skill_input)
        if preview or getattr(self, 'preview', False):
            preview_skill(market_share_analysis, out)
        return out

    def _assert_market_share_analysis_runs_with_error(self, parameters: Dict, expected_exception):
        try:
            self._run_market_share_analysis(parameters, preview=False)
            assert False, f"Expected exception but skill ran successfully"
        except expected_exception as e:
            pass
        except Exception as e:
            assert False, f"Expected {expected_exception}, got {type(e).__name__}: {e}"

    def _assert_market_share_analysis_runs_without_errors(self, parameters: Dict, preview: bool = False):
        self._run_market_share_analysis(parameters, preview=preview)
        assert True


class TestMarketShareAnalysisCommonParameters(TestMarketShareAnalysis):
    """Test the market share analysis skill with common parameters to verify functionality"""

    config = PastaV9MarketShareAnalysisCommonParametersConfig
    preview = False

    def test_single_metric_with_periods_and_subject_filter(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.share_metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_growth_type_yoy(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.growth_type.value: self.config.growth_type_yoy
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_growth_type_pop(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.growth_type.value: self.config.growth_type_pop
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_filters(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_custom_limit_n(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.limit_n.value: 10
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_global_view(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.global_view.value: self.config.valid_global_view
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_market_view(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.market_view.value: self.config.valid_market_view
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_include_drivers_true(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.include_drivers.value: True
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_include_drivers_false(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.include_drivers.value: False
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_market_cols(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.market_cols.value: self.config.valid_market_cols
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_impact_calcs(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.impact_calcs.value: self.config.valid_impact_calcs
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_decomposition_display_config(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.decomposition_display_config.value: self.config.valid_decomposition_display_config
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_single_metric_with_subject_metric_config(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.subject_metric_config.value: self.config.valid_subject_metric_config
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_share_metric_basic(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.share_metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_complex_parameter_combination(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.growth_type.value: self.config.growth_type_yoy,
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.limit_n.value: 15,
            MarketShareAnalysisParameters.global_view.value: self.config.valid_global_view,
            MarketShareAnalysisParameters.market_view.value: self.config.valid_market_view,
            MarketShareAnalysisParameters.include_drivers.value: True,
            MarketShareAnalysisParameters.impact_calcs.value: self.config.valid_impact_calcs
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_multiple_periods(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: ["2022", "2023"],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.growth_type.value: self.config.growth_type_yoy
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_multiple_filters(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1, self.config.filter_2]
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)


class TestMarketShareAnalysisGuardrails(TestMarketShareAnalysis):
    """Test guardrails and error conditions for market share analysis skill"""

    config = PastaV9MarketShareAnalysisCommonParametersConfig
    guardrail_config = PastaV9MarketShareAnalysisGuardrailsConfig
    preview = False

    def test_no_metric_provided(self):
        parameters = {
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter]
        }
        self._assert_market_share_analysis_runs_with_error(parameters, ExitFromSkillException)

    def test_no_subject_filter_provided(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter]
        }
        self._assert_market_share_analysis_runs_with_error(parameters, ExitFromSkillException)

    def test_invalid_metric(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.guardrail_config.invalid_metric,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_market_share_analysis_runs_with_error(parameters, ExitFromSkillException)

    def test_malformed_json_global_view(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.global_view.value: self.guardrail_config.malformed_json
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_invalid_json_syntax_market_view(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.market_view.value: self.guardrail_config.invalid_json_syntax
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_invalid_growth_type_defaults_to_yoy(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [self.config.period_filter],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1],
            MarketShareAnalysisParameters.growth_type.value: self.guardrail_config.invalid_growth_type
        }
        self._assert_market_share_analysis_runs_without_errors(parameters)

    def test_no_periods_provided(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1
        }
        self._assert_market_share_analysis_runs_with_error(parameters, ExitFromSkillException)

    def test_empty_periods_list(self):
        parameters = {
            MarketShareAnalysisParameters.metric.value: self.config.metric_1,
            MarketShareAnalysisParameters.periods.value: [],
            MarketShareAnalysisParameters.other_filters.value: [self.config.filter_1]
        }
        self._assert_market_share_analysis_runs_with_error(parameters, ExitFromSkillException)