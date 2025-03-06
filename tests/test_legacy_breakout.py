from dimension_breakout import simple_breakout
from skill_framework import SkillInput
from skill_framework.preview import preview_skill


class TestLegacyBreakout:

    def test_simple_breakout_skill(self):
        '''
        Checks to see if the simple breakout skill runs without errors
        '''

        # currently assumes it's attached to the pasta dataset

        skill_input: SkillInput = simple_breakout.create_input(arguments={'metrics': ["sales", "volume"], 'breakouts': ["brand", "manufacturer"], 'periods': ["2022"], 'growth_type': "Y/Y", 'other_filters': []})
        out = simple_breakout(skill_input)
        preview_skill(simple_breakout, out)

        assert True
