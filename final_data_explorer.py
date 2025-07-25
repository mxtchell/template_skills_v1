import json
from jinja2 import Template
from skill_framework import preview_skill, skill, SkillParameter, SkillInput, SkillOutput
from data_explorer_helper.data_explorer_config import FINAL_PROMPT_TEMPLATE, DATA_EXPLORE_LAYOUT, SQL_ERROR_FINAL_PROMPT_TEMPLATE, SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT
from data_explorer_helper.data_explorer_functionality import run_data_explorer


@skill(
    name="Final Data Explorer",
    description="A data explorer skill that returns customizable Jinja template format with SQL query extraction. Uses template with message and json_table variables.",
    capabilities="Generates SQL queries from natural language, returns data using customizable Jinja template format. Template variables: message, json_table. Includes SQL query in output.",
    limitations="Only works within one dataset at a time. Returns data using Jinja template formatting.",
    parameters=[
        SkillParameter(
            name="user_chat_question_with_context",
            description="The user's request for data, rephrased to include conversation context if applicable",
            required=True
        ),
        SkillParameter(
            name="final_prompt_template",
            parameter_type="prompt",
            description="Jinja template for response format. Available variables: message, json_table",
            default_value="{{ message }}\n\n%BEGIN_JSON%\n{{ json_table }}\n%END_JSON%"
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
def final_data_explorer(parameters: SkillInput) -> SkillOutput:
    """
    Final Data Explorer skill

    Returns data using customizable Jinja template format with SQL extraction.
    Template variables available: message, json_table
    """
    
    print("DEBUG: Starting Final Data Explorer skill")
    print(f"DEBUG: Parameters: {parameters.arguments}")
    print(f"DEBUG: Template: {parameters.arguments.final_prompt_template}")
    
    # Add more detailed debugging for result object structure
    print("DEBUG: Inspecting result object structure...")
    
    # Call the original data explorer functionality
    try:
        result = run_data_explorer(parameters)
        print(f"DEBUG: Original result type: {type(result)}")
        print(f"DEBUG: Original result has export_data: {hasattr(result, 'export_data')}")
        
        if hasattr(result, 'export_data') and result.export_data:
            print(f"DEBUG: Export data count: {len(result.export_data)}")
    except Exception as e:
        print(f"DEBUG: Error in run_data_explorer: {e}")
        # Return error using template format
        error_message = f"Error executing query: {str(e)}"
        error_table = {
            "data_type": "table",
            "data": [],
            "columns": [],
            "sql": ""
        }
        
        template_str = parameters.arguments.final_prompt_template
        template = Template(template_str)
        final_output = template.render(
            message=error_message,
            json_table=json.dumps(error_table, indent=2)
        )
        
        return SkillOutput(
            final_prompt=final_output,
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
        
        # Get the SQL query from the result if available
        sql_query = ""
        
        # Method 1: Extract SQL from final_prompt (most reliable)
        if hasattr(result, 'final_prompt') and result.final_prompt:
            final_prompt = result.final_prompt
            # Look for SQL in markdown code blocks
            import re
            sql_patterns = [
                r'```sql\n(.*?)\n```',  # Standard SQL markdown block
                r'```SQL\n(.*?)\n```',  # Uppercase SQL block
                r'The following SQL query was executed:\s*(.*?)\n\n',  # Common pattern
                r'SQL:\s*(SELECT.*?)(?:\n\n|\Z)',  # SQL: followed by SELECT
            ]
            
            for pattern in sql_patterns:
                matches = re.findall(pattern, final_prompt, re.DOTALL | re.IGNORECASE)
                if matches:
                    sql_query = matches[0].strip()
                    break
        
        # Method 2: Try to get SQL from visualizations layout variables
        if not sql_query and hasattr(result, 'visualizations') and result.visualizations:
            print(f"DEBUG: Found {len(result.visualizations)} visualizations")
            for i, viz in enumerate(result.visualizations):
                print(f"DEBUG: Viz {i} has layout_variables: {hasattr(viz, 'layout_variables')}")
                print(f"DEBUG: Viz {i} attributes: {[attr for attr in dir(viz) if not attr.startswith('_')]}")
                print(f"DEBUG: Viz {i} type: {type(viz)}")
                
                # Check if there's a layout or template attribute
                if hasattr(viz, 'layout'):
                    print(f"DEBUG: Viz {i} has layout: {type(viz.layout)}")
                    if isinstance(viz.layout, str) and ('```sql' in viz.layout or 'SELECT' in viz.layout.upper()):
                        print(f"DEBUG: FOUND SQL in viz.layout!")
                        print(f"DEBUG: Layout snippet: {viz.layout[:500]}...")
                        import re
                        # Try multiple regex patterns based on the JSON structure we see
                        patterns = [
                            r'"text":\s*"```sql\\n(.*?)\\n```"',  # Most likely format - full SQL block
                            r'"text":\s*"```sql\\n(.*?)"',  # SQL block without closing ```
                            r'(SELECT.*?LIMIT.*?)(?=")',  # Complete SELECT statement until LIMIT and quote
                            r'(SELECT.*?)(?=")',  # SELECT until closing quote
                            r'```sql\\n(.*?)\\n```',
                            r'```sql\n(.*?)\n```',
                        ]
                        
                        for pattern in patterns:
                            match = re.search(pattern, viz.layout, re.DOTALL | re.IGNORECASE)
                            if match:
                                sql_query = match.group(1).strip()
                                print(f"DEBUG: Extracted SQL with pattern '{pattern}': {sql_query[:100]}...")
                                break
                        
                        if sql_query:
                            break
                
                if hasattr(viz, 'layout_variables') and viz.layout_variables:
                    print(f"DEBUG: Viz {i} layout_variables keys: {list(viz.layout_variables.keys())}")
                    # Check all layout variables for SQL content
                    for key, value in viz.layout_variables.items():
                        print(f"DEBUG: Checking key '{key}': {type(value)} - {str(value)[:100]}...")
                        if isinstance(value, str) and ('```sql' in value or 'SELECT' in value.upper()):
                            print(f"DEBUG: FOUND SQL in key '{key}'!")
                            sql_text = value
                            # Remove markdown formatting: ```sql\n ... \n```
                            if sql_text.startswith('```sql\n'):
                                sql_query = sql_text[7:]  # Remove ```sql\n
                                if sql_query.endswith('\n```'):
                                    sql_query = sql_query[:-4]  # Remove \n```
                            elif '```sql' in sql_text:
                                # Handle other markdown formats
                                import re
                                match = re.search(r'```sql\n(.*?)```', sql_text, re.DOTALL)
                                if match:
                                    sql_query = match.group(1).strip()
                            else:
                                sql_query = sql_text
                            break
                    if sql_query:
                        break
        
        # Method 3: Try to extract from export_data metadata
        if not sql_query and result.export_data and len(result.export_data) > 0:
            first_export = result.export_data[0]
            if hasattr(first_export, 'sql') and first_export.sql:
                sql_query = first_export.sql
            elif hasattr(first_export, 'metadata') and first_export.metadata:
                if 'sql' in first_export.metadata:
                    sql_query = first_export.metadata['sql']
        
        print(f"DEBUG: Found SQL query: {sql_query[:100] if sql_query else 'None'}...")
        
        table_structure = {
            "data_type": "table",
            "data": table_data,
            "columns": columns,
            "sql": sql_query
        }
        
    else:
        # No data case
        message = f"No data found for: {user_question}"
        table_structure = {
            "data_type": "table",
            "data": [],
            "columns": [],
            "sql": ""
        }
    
    # Convert table structure to JSON string (the "table" field content)
    json_table = json.dumps(table_structure, indent=2)
    
    print(f"DEBUG: Created response with:")
    print(f"  - message length: {len(message)}")
    print(f"  - table rows: {len(table_structure['data'])}")
    print(f"  - table columns: {len(table_structure['columns'])}")
    
    # Use Jinja template for final output
    template_str = parameters.arguments.final_prompt_template
    template = Template(template_str)
    final_output = template.render(
        message=message,
        json_table=json_table
    )
    
    print(f"DEBUG: Final output length: {len(final_output)}")
    
    # Return the templated output
    return SkillOutput(
        final_prompt=final_output,
        narrative=message,
        visualizations=result.visualizations,
        export_data=result.export_data,
        parameter_display_descriptions=result.parameter_display_descriptions if hasattr(result, 'parameter_display_descriptions') else [],
        followup_questions=result.followup_questions if hasattr(result, 'followup_questions') else []
    )
    
    
if __name__ == '__main__':
    mock_input = final_data_explorer.create_input(arguments={
        'user_chat_question_with_context': "show me sales by month", 
        "question_is_follow_up": False
    })
    output = final_data_explorer(mock_input)
    preview_skill(final_data_explorer, output)
    print("Final prompt output:")
    print(output.final_prompt)