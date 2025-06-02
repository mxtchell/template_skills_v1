from skill_framework import preview_skill, skill, SkillParameter, SkillInput, SkillOutput
from data_explorer_helper.data_explorer_config import FINAL_PROMPT_TEMPLATE, DATA_EXPLORE_LAYOUT, SQL_ERROR_FINAL_PROMPT_TEMPLATE, SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT
from data_explorer_helper.data_explorer_functionality import run_data_explorer


@skill(
    name="Data Explorer",
    description="A skill that can generate SQL queries from a natural language user query, returns the retrieved data and the SQL query used to retrieve it. Modifications to the visualization must be made via follow-up questions, the user does not have any ability to modify the visualization directly. If the user wants changes to the visualization, you need to run this skill again",
    capabilities="This skill is able to generate SQL queries from a natural language user query, return the retrieved data and the SQL query used to retrieve it, and attempt to generate a visualization from the retrieved data.",
    limitations="Only works within one dataset at a time. Only produces 1 chart and 1 table at a time. Can only retrieve data that exists within the dataset the skill is connected to. Unable to make modifications or transformations on the retrieved data directly, only through generating a new SQL query.",
    parameters=[
        SkillParameter(
            name="user_chat_question_with_context",
            description="The user's request for data, rephrased to include conversation context if applicable",
            required=True
        ),
        SkillParameter(
            name="final_prompt_template",
            parameter_type="prompt",
            description="The prompt template used for Max's response when the SQL service executes successfully",
            default_value=FINAL_PROMPT_TEMPLATE
        ),
        SkillParameter(
            name="sql_error_final_prompt_template",
            parameter_type="prompt",
            description="The prompt template used for Max's response when the SQL service returns an error",
            default_value=SQL_ERROR_FINAL_PROMPT_TEMPLATE
        ),
        SkillParameter(
            name="sql_success_empty_data_final_prompt",
            parameter_type="prompt",
            description="The prompt used for Max response when the SQL service returns an empty dataframe",
            default_value=SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT
        ),
        SkillParameter(
            name="data_explore_vis_layout",
            parameter_type="visualization",
            description="The vis layout for the artifact panel",
            default_value=DATA_EXPLORE_LAYOUT
        )
    ]
)
def data_explorer(parameters: SkillInput):
    """
    Data Explorer skill

    This skill is able to generate SQL queries from a natural language user query, return the retrieved data and the SQL query used to retrieve it, and attempt to generate a visualization from the retrieved data.

    """
    return run_data_explorer(parameters)
    
    
if __name__ == '__main__':
    mock_input = data_explorer.create_input(arguments={'user_chat_question_with_context': "show me sales over time for the top 5 segments in 2022", "question_is_follow_up": False})
    output = data_explorer(mock_input)
    preview_skill(data_explorer, output)
    print(output)