import json
from skill_framework import preview_skill, skill, SkillParameter, SkillInput, SkillOutput
from data_explorer_helper.data_explorer_config import FINAL_PROMPT_TEMPLATE, DATA_EXPLORE_LAYOUT, SQL_ERROR_FINAL_PROMPT_TEMPLATE, SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT
from data_explorer_helper.data_explorer_functionality import run_data_explorer


@skill(
    name="MetricInsights Data Explorer",
    description="A data explorer skill that returns clean JSON format for MetricInsights integration. Returns structured data without LLM instructions.",
    capabilities="Generates SQL queries from natural language, returns data in clean JSON format with message and table properties. Designed for programmatic consumption.",
    limitations="Only works within one dataset at a time. Returns data in JSON format optimized for external agent processing.",
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
def metricinsights_data_explorer(parameters: SkillInput) -> SkillOutput:
    """
    MetricInsights Data Explorer skill

    Returns clean JSON format optimized for external agent processing.
    Removes LLM instructions and provides structured data output.
    """
    
    print("DEBUG: Starting MetricInsights Data Explorer skill")
    print(f"DEBUG: Parameters: {parameters.arguments}")
    
    # Call the original data explorer functionality
    try:
        result = run_data_explorer(parameters)
        print(f"DEBUG: Original result type: {type(result)}")
        print(f"DEBUG: Original result has export_data: {hasattr(result, 'export_data')}")
        
        if hasattr(result, 'export_data') and result.export_data:
            print(f"DEBUG: Export data count: {len(result.export_data)}")
    except Exception as e:
        print(f"DEBUG: Error in run_data_explorer: {e}")
        # Return error in JSON format
        error_response = {
            "message": f"Error executing query: {str(e)}",
            "table": {
                "data_type": "table",
                "data": [],
                "columns": []
            }
        }
        
        return SkillOutput(
            final_prompt=json.dumps(error_response, indent=2),
            narrative="Error occurred during data exploration",
            visualizations=result.visualizations if 'result' in locals() else [],
            export_data=result.export_data if 'result' in locals() else []
        )
    
    # Extract the DataFrame from export_data
    dataframe = None
    if result.export_data and len(result.export_data) > 0:
        # Get the first export data item (usually the main table)
        first_export = result.export_data[0]
        dataframe = first_export.data
        print(f"DEBUG: Found DataFrame with shape: {dataframe.shape}")
        print(f"DEBUG: DataFrame columns: {dataframe.columns.tolist()}")
    else:
        print("DEBUG: No export data found")
    
    # Create clean message without LLM instructions
    user_question = parameters.arguments.user_chat_question_with_context
    
    if dataframe is not None and not dataframe.empty:
        # Success case with data
        row_count = len(dataframe)
        col_count = len(dataframe.columns)
        message = f"Retrieved {row_count} rows and {col_count} columns for: {user_question}"
        
        # Convert DataFrame to required JSON format
        table_data = dataframe.to_dict('records')
        
        # Create columns metadata
        columns = []
        for col in dataframe.columns:
            columns.append({
                "key": col,
                "label": col.replace('_', ' ').title()
            })
        
        table_structure = {
            "data_type": "table",
            "data": table_data,
            "columns": columns
        }
        
    else:
        # No data case
        message = f"No data found for: {user_question}"
        table_structure = {
            "data_type": "table",
            "data": [],
            "columns": []
        }
    
    # Create the final JSON response
    json_response = {
        "message": message,
        "table": table_structure
    }
    
    print(f"DEBUG: Created JSON response with:")
    print(f"  - message length: {len(json_response['message'])}")
    print(f"  - table rows: {len(json_response['table']['data'])}")
    print(f"  - table columns: {len(json_response['table']['columns'])}")
    
    # Convert to JSON string for final_prompt
    json_string = json.dumps(json_response, indent=2)
    
    print(f"DEBUG: JSON string length: {len(json_string)}")
    
    # Return the JSON as the final prompt
    return SkillOutput(
        final_prompt=json_string,
        narrative=message,
        visualizations=result.visualizations,
        export_data=result.export_data,
        parameter_display_descriptions=result.parameter_display_descriptions if hasattr(result, 'parameter_display_descriptions') else [],
        followup_questions=result.followup_questions if hasattr(result, 'followup_questions') else []
    )
    
    
if __name__ == '__main__':
    mock_input = metricinsights_data_explorer.create_input(arguments={
        'user_chat_question_with_context': "show me sales by month", 
        "question_is_follow_up": False
    })
    output = metricinsights_data_explorer(mock_input)
    preview_skill(metricinsights_data_explorer, output)
    print("Final prompt output:")
    print(output.final_prompt)