from typing import Dict
from metric_drivers import simple_metric_driver
from skill_framework import SkillInput, ExitFromSkillException
from skill_framework.preview import preview_skill


class TestMetricDrivers:

    # TODO: Can this test be made generic and put into ar-analytics?

    met1 = "sales"
    met2 = "volume"
    # sales_met = "sales_share" # todo: need this for overproof?
    breakout1 = "brand"
    breakout2 = "base_size"
    period_filter1 = "2023"
    growth_type = "Y/Y"
    filter1 = {"dim": "brand", "op": "=", "val": "barilla"}
    filter2 = {"dim": "base_size", "op": "=", "val": "12 ounce"}

    preview = True # Set to True to get previews

    def _run_metric_drivers(self, parameters: Dict, preview: bool = False):

        skill_input: SkillInput = simple_metric_driver.create_input(arguments=parameters)
        out = simple_metric_driver(skill_input)
        if preview or self.preview:
            preview_skill(simple_metric_driver, out)

        return out

    def _assert_metric_drivers_runs_with_error(self, parameters: Dict, expected_exception: Exception):

        try:
            self._run_metric_drivers(parameters, preview=False)
        except Exception as e:
            assert isinstance(e, expected_exception)

    def _assert_metric_drivers_runs_without_errors(self, parameters: Dict, preview: bool = False):
        
        self._run_metric_drivers(parameters, preview=preview)

        assert True

    def test_single_metric_with_period(self):
        """Test with a single metric, no growth type"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1]
        }

        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_growth_type(self):
        """Test with a single metric, growth type"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1],
            "growth_type": self.growth_type
        }

        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_breakout(self):
        """Test with a single metric, breakout"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1],
            "breakouts": [self.breakout1]
        }

        self._assert_metric_drivers_runs_without_errors(parameters)

    def test_single_metric_with_period_and_filter(self):
        """Test with a single metric, period, and filter"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1],
            "other_filters": [self.filter1]
        }

        self._assert_metric_drivers_runs_with_error(parameters, ExitFromSkillException)

    def test_single_metric_with_period_and_breakout_and_filter(self):
        """Test with a single metric, period, breakout, and filter"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1],
            "breakouts": [self.breakout1],
            "other_filters": [self.filter1]
        }

        self._assert_metric_drivers_runs_without_errors(parameters)  
        
    def test_single_metric_with_period_and_breakout_and_filter_and_filter2(self):
        """Test with a single metric, period, breakout, filter, and filter2"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1],
            "breakouts": [self.breakout1],
            "other_filters": [self.filter1, self.filter2]
        }

        self._assert_metric_drivers_runs_without_errors(parameters)
        
    def test_single_metric_with_period_and_breakout_and_filter_and_filter2_and_growth_type(self):
        """Test with a single metric, period, breakout, filter, filter2, and growth type"""

        parameters = {
            "metric": self.met1,
            "periods": [self.period_filter1],
            "breakouts": [self.breakout1],
            "other_filters": [self.filter1, self.filter2],
            "growth_type": self.growth_type
        }

        self._assert_metric_drivers_runs_without_errors(parameters)
