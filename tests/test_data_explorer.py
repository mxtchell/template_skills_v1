from data_explorer import data_explorer
from skill_framework import SkillInput
from skill_framework.preview import preview_skill


class TestDataExplorer:
    def test_data_explorer_skill(self):
        skill_input: SkillInput = data_explorer.create_input(arguments={"user_chat_question_with_context": "venues in vermont"})
        out = data_explorer(skill_input)
        preview_skill(data_explorer, out)

        assert True

if __name__ == '__main__':
    TestDataExplorer().test_data_explorer_skill()

