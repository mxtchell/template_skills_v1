from typing import Dict
from market_share_analysis import market_share_analysis
from skill_framework import SkillInput
from skill_framework.preview import preview_skill


class TestMarketShareAnalysis:

    metric_sales_share = "sales_share"
    period_filter_2024 = "2023"
    growth_type = "Y/Y"
    barilla_filter = {"dim": "brand", "op": "=", "val": "barilla"}
    private_label_filter = {"dim": "manufacturer", "op": "=", "val": "private label"} 

    preview = True

    def _run_msa(self, parameters: Dict, preview: bool = False):

        skill_input: SkillInput = market_share_analysis.create_input(arguments=parameters)
        out = market_share_analysis(skill_input)
        if preview or self.preview:
            preview_skill(market_share_analysis, out)

        return out
    
    def _assert_msa_runs_without_errors(self, parameters: Dict, preview: bool = False):
        
        self._run_msa(parameters, preview=preview)

        assert True
    
    def test_sales_share_in_2024_for_barilla(self):
        """Test with a single metric, no growth type"""

        parameters = {
            "metric": self.metric_sales_share,
            "periods": [self.period_filter_2024],
            "other_filters": [self.barilla_filter]
        }

        self._assert_msa_runs_without_errors(parameters)

    def test_sales_share_in_2024_for_private_label(self):
        """Test with a single metric, no growth type"""

        parameters = {
            "metric": self.metric_sales_share,
            "periods": [self.period_filter_2024],
            "other_filters": [self.private_label_filter]
        }

        self._assert_msa_runs_without_errors(parameters)
