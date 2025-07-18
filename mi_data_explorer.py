import json
from jinja2 import Template
from skill_framework import preview_skill, skill, SkillParameter, SkillInput, SkillOutput
from data_explorer_helper.data_explorer_config import FINAL_PROMPT_TEMPLATE, DATA_EXPLORE_LAYOUT, SQL_ERROR_FINAL_PROMPT_TEMPLATE, SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT
from data_explorer_helper.data_explorer_functionality import run_data_explorer


@skill(
    name="MI Data Explorer",
    description="A data explorer skill that returns both Highcharts visualization and table data together for MetricInsights integration. Provides comprehensive chart and tabular data response.",
    capabilities="Generates SQL queries from natural language, returns both hchart format (charts) and table format data in a single response. Includes SQL query extraction for tooltips.",
    limitations="Only works within one dataset at a time. Returns data optimized for MetricInsights integration.",
    parameters=[
        SkillParameter(
            name="user_chat_question_with_context",
            description="The user's request for data, rephrased to include conversation context if applicable",
            required=True
        ),
        SkillParameter(
            name="final_prompt_template",
            parameter_type="prompt",
            description="Jinja template for response format. Available variables: message, json_chart, json_table",
            default_value="{{ message }}\n\n%BEGIN_JSON%\n{{ json_chart }}\n%END_JSON%\n\n%BEGIN_JSON%\n{{ json_table }}\n%END_JSON%"
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
def mi_data_explorer(parameters: SkillInput) -> SkillOutput:
    """
    MI Data Explorer skill

    Returns both Highcharts visualization and table data together for MetricInsights integration.
    Provides comprehensive chart and tabular data response with SQL query.
    """
    
    print("DEBUG: Starting MI Data Explorer skill")
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
        # Return error using combined format
        error_message = f"Error executing query: {str(e)}"
        
        error_chart = {
            "data_type": "hchart",
            "data": {
                "chart_type": "ERROR_CHART",
                "highChartsOptions": "{}",
                "title": "Error Generating Chart",
                "type": "chart"
            },
            "sql": ""
        }
        
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
            json_chart=json.dumps(error_chart, indent=2),
            json_table=json.dumps(error_table, indent=2)
        )
        
        return SkillOutput(
            final_prompt=final_output,
            narrative="Error occurred during data exploration",
            visualizations=result.visualizations if 'result' in locals() else [],
            export_data=result.export_data if 'result' in locals() else []
        )
    
    user_question = parameters.arguments.user_chat_question_with_context
    
    # Extract chart data from visualizations
    chart_data = None
    chart_title = ""
    chart_type = "LINE_CHART"  # Default
    
    if hasattr(result, 'visualizations') and result.visualizations:
        print(f"DEBUG: Found {len(result.visualizations)} visualizations")
        for i, viz in enumerate(result.visualizations):
            print(f"DEBUG: Viz {i} type: {type(viz)}")
            
            # Try to extract chart data from visualization
            if hasattr(viz, 'layout') and isinstance(viz.layout, str):
                print(f"DEBUG: Found layout in visualization {i}")
                try:
                    import json
                    layout_data = json.loads(viz.layout)
                    print(f"DEBUG: Layout data keys: {list(layout_data.keys()) if isinstance(layout_data, dict) else 'Not a dict'}")
                    
                    # Look for Highcharts configuration in layout
                    if isinstance(layout_data, dict):
                        # Search for Highcharts chart element
                        chart_config = None
                        def find_highcharts_config(obj, path=""):
                            if isinstance(obj, dict):
                                if obj.get('type') == 'HighchartsChart':
                                    print(f"DEBUG: Found HighchartsChart at {path}")
                                    return obj.get('options', {})
                                
                                for key, value in obj.items():
                                    result_config = find_highcharts_config(value, f"{path}.{key}")
                                    if result_config:
                                        return result_config
                            elif isinstance(obj, list):
                                for idx, item in enumerate(obj):
                                    result_config = find_highcharts_config(item, f"{path}[{idx}]")
                                    if result_config:
                                        return result_config
                            return None
                        
                        chart_config = find_highcharts_config(layout_data)
                        if chart_config:
                            # Use the chart data from visualization service
                            chart_data = chart_config
                            chart_title = chart_config.get('title', {}).get('text', f"Chart for: {user_question}")
                            
                            # Determine chart type from config
                            if chart_config.get('chart', {}).get('type') == 'pie':
                                chart_type = "PIE_CHART"
                            elif chart_config.get('chart', {}).get('type') == 'bar':
                                chart_type = "BAR_CHART"
                            elif chart_config.get('chart', {}).get('type') == 'column':
                                chart_type = "COLUMN_CHART"
                            else:
                                chart_type = "LINE_CHART"
                            
                            print(f"DEBUG: Extracted chart type: {chart_type}")
                            break
                        else:
                            print(f"DEBUG: No HighchartsChart found in layout {i}")
                            
                except json.JSONDecodeError:
                    print(f"DEBUG: Failed to parse layout as JSON for viz {i}")
                    continue
    else:
        print("DEBUG: No visualizations found")
    
    # Extract table data from export_data
    dataframe = None
    if result.export_data and len(result.export_data) > 0:
        first_export = result.export_data[0]
        dataframe = first_export.data
        print(f"DEBUG: Found DataFrame with shape: {dataframe.shape}")
    else:
        print("DEBUG: No export data found")
    
    # Create table structure
    if dataframe is not None and not dataframe.empty:
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
        
        row_count = len(dataframe)
        col_count = len(dataframe.columns)
        table_message = f"Retrieved {row_count} rows and {col_count} columns"
        
    else:
        # No data case
        table_structure = {
            "data_type": "table",
            "data": [],
            "columns": []
        }
        table_message = "No data found"
    
    # Create chart structure
    if chart_data:
        chart_structure = {
            "data_type": "hchart",
            "data": {
                "chart_type": chart_type,
                "highChartsOptions": json.dumps(chart_data),
                "title": chart_title,
                "type": "chart"
            }
        }
        chart_message = f"Generated chart visualization"
    else:
        chart_structure = {
            "data_type": "hchart",
            "data": {
                "chart_type": "EMPTY_CHART",
                "highChartsOptions": "{}",
                "title": "No Chart Available",
                "type": "chart"
            }
        }
        chart_message = "No chart visualization available"
    
    # Extract SQL query - try multiple sources
    sql_query = ""
    
    # Method 1: Extract from final_prompt
    if hasattr(result, 'final_prompt') and result.final_prompt:
        final_prompt = result.final_prompt
        import re
        sql_patterns = [
            r'```sql\\n(.*?)\\n```',  # Standard SQL markdown block
            r'```SQL\\n(.*?)\\n```',  # Uppercase SQL block
            r'The following SQL query was executed:\\s*(.*?)\\n\\n',  # Common pattern
            r'SQL:\\s*(SELECT.*?)(?:\\n\\n|\\Z)',  # SQL: followed by SELECT
        ]
        
        for pattern in sql_patterns:
            matches = re.findall(pattern, final_prompt, re.DOTALL | re.IGNORECASE)
            if matches:
                sql_query = matches[0].strip()
                break
    
    # Method 2: Extract from visualizations layout (most reliable)
    if not sql_query and hasattr(result, 'visualizations') and result.visualizations:
        for i, viz in enumerate(result.visualizations):
            if hasattr(viz, 'layout') and isinstance(viz.layout, str):
                try:
                    import json
                    layout_data = json.loads(viz.layout)
                    # Look for SQL in layout text elements
                    import re
                    sql_patterns = [
                        r'"text":\s*"```sql\\\\n(.*?)\\\\n```"',  # Most common format
                        r'"text":\s*"```sql\\\\n(.*?)"',  # SQL block without closing
                        r'(SELECT.*?LIMIT.*?)(?=")',  # Complete SELECT with LIMIT
                        r'(SELECT.*?)(?=")',  # Any SELECT statement
                    ]
                    
                    for pattern in sql_patterns:
                        matches = re.findall(pattern, viz.layout, re.DOTALL | re.IGNORECASE)
                        if matches:
                            sql_query = matches[0].strip()
                            # Clean up escaped characters
                            sql_query = sql_query.replace('\\n', '\n').replace('\\"', '"')
                            break
                    
                    if sql_query:
                        break
                        
                except json.JSONDecodeError:
                    continue
    
    # Method 3: Extract from export_data metadata
    if not sql_query and result.export_data and len(result.export_data) > 0:
        first_export = result.export_data[0]
        if hasattr(first_export, 'sql') and first_export.sql:
            sql_query = first_export.sql
        elif hasattr(first_export, 'metadata') and first_export.metadata:
            if 'sql' in first_export.metadata:
                sql_query = first_export.metadata['sql']
    
    print(f"DEBUG: Found SQL query: {sql_query[:100] if sql_query else 'None'}...")
    
    # Create separate JSON responses for chart and table
    chart_response = {
        "data_type": "hchart",
        "data": {
            "chart_type": chart_structure["data"]["chart_type"],
            "highChartsOptions": chart_structure["data"]["highChartsOptions"],
            "title": chart_structure["data"]["title"],
            "type": "chart"
        },
        "sql": sql_query
    }
    
    table_response = {
        "data_type": "table",
        "data": table_structure["data"],
        "columns": table_structure["columns"],
        "sql": sql_query
    }
    
    json_chart = json.dumps(chart_response, indent=2)
    json_table = json.dumps(table_response, indent=2)
    message = f"{chart_message} and {table_message} for: {user_question}"
    
    print(f"DEBUG: Created separate chart and table JSON responses")
    print(f"DEBUG: Chart response length: {len(json_chart)}")
    print(f"DEBUG: Table response length: {len(json_table)}")
    
    # Use Jinja template for final output
    template_str = parameters.arguments.final_prompt_template
    template = Template(template_str)
    final_output = template.render(
        message=message,
        json_chart=json_chart,
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
    mock_input = mi_data_explorer.create_input(arguments={
        'user_chat_question_with_context': "show me sales by month", 
        "question_is_follow_up": False
    })
    output = mi_data_explorer(mock_input)
    preview_skill(mi_data_explorer, output)
    print("Final prompt output:")
    print(output.final_prompt)