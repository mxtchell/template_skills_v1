from trend import trend
from skill_framework import SkillInput
from skill_framework.preview import preview_skill


class TestTrend:

    def test_full_trend_skill(self):
        '''
        Checks to see if the full trend skill runs without errors
        '''

        # currently assumes it's attached to the pasta dataset

        skill_input: SkillInput = trend.create_input(arguments={'metrics': ["sales", "volume"], 'breakouts': [], 'periods': ["2022"], 'growth_type': "Y/Y"})
        out = trend(skill_input)
        preview_skill(trend, out)

        assert True
