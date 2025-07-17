import json
from jinja2 import Template
from skill_framework import preview_skill, skill, SkillParameter, SkillInput, SkillOutput
from data_explorer_helper.data_explorer_config import FINAL_PROMPT_TEMPLATE, DATA_EXPLORE_LAYOUT, SQL_ERROR_FINAL_PROMPT_TEMPLATE, SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT
from data_explorer_helper.data_explorer_functionality import run_data_explorer


def is_chart_data_valid(chart_config):
    """
    Validate chart data to detect corrupted/placeholder values
    """
    if not chart_config or not isinstance(chart_config, dict):
        return False
    
    # Check for series data
    series = chart_config.get('series', [])
    if not series:
        return False
    
    # Common placeholder/corrupted values to watch for
    suspicious_names = [
        "sample", "chart", "this", "is", "a", "test", "example", 
        "data", "value", "item", "element", "field", "column"
    ]
    
    for serie in series:
        if isinstance(serie, dict) and 'data' in serie:
            data_points = serie['data']
            if isinstance(data_points, list):
                # Check for suspicious names in pie chart data
                for point in data_points:
                    if isinstance(point, dict) and 'name' in point:
                        name = str(point['name']).lower().strip()
                        if any(suspicious in name for suspicious in suspicious_names):
                            print(f"DEBUG: Found suspicious data point name: '{point['name']}'")
                            return False
                        # Check for very generic single-word names
                        if len(name) <= 2 and name.isalpha():
                            print(f"DEBUG: Found very short suspicious name: '{point['name']}'")
                            return False
    
    return True


def should_create_pie_chart(user_question, dataframe):
    """
    Detect if this query should generate a pie chart based on question and data structure
    """
    if dataframe is None or dataframe.empty:
        return False
    
    # Check if question contains pie chart keywords
    pie_keywords = [
        'percentage', 'percent', '%', 'share', 'distribution', 'breakdown', 
        'composition', 'proportion', 'split', 'pie', 'comes from each'
    ]
    
    question_lower = user_question.lower()
    has_pie_keywords = any(keyword in question_lower for keyword in pie_keywords)
    
    # Check if data structure is suitable for pie chart
    # Should have exactly 2 columns: one category, one metric
    if len(dataframe.columns) != 2:
        return False
    
    # Check column types - first should be categorical, second should be numeric
    first_col = dataframe.columns[0]
    second_col = dataframe.columns[1]
    
    # Check if second column contains percentage/share data
    second_col_lower = second_col.lower()
    has_percentage_column = any(keyword in second_col_lower for keyword in ['%', 'percent', 'share'])
    
    # Check if first column has reasonable number of categories for pie chart (2-15)
    unique_categories = dataframe[first_col].nunique()
    reasonable_categories = 2 <= unique_categories <= 15
    
    print(f"DEBUG: Pie chart detection - keywords: {has_pie_keywords}, percentage col: {has_percentage_column}, categories: {unique_categories}")
    
    return has_pie_keywords and reasonable_categories


def create_pie_chart_config(dataframe, user_question):
    """
    Manually create a Highcharts pie chart configuration from DataFrame
    """
    if dataframe is None or dataframe.empty or len(dataframe.columns) != 2:
        return None
    
    category_col = dataframe.columns[0]
    value_col = dataframe.columns[1]
    
    # Create pie chart data points
    data_points = []
    for _, row in dataframe.iterrows():
        data_points.append({
            "name": str(row[category_col]),
            "y": float(row[value_col])
        })
    
    # Create Highcharts pie chart configuration
    chart_config = {
        "chart": {
            "type": "pie",
            "height": 400
        },
        "title": {
            "text": f"Distribution by {category_col}",
            "style": {
                "fontSize": "18px",
                "fontWeight": "bold"
            }
        },
        "plotOptions": {
            "pie": {
                "allowPointSelect": True,
                "cursor": "pointer",
                "dataLabels": {
                    "enabled": True,
                    "format": "{point.name}: {point.percentage:.1f}%",
                    "style": {
                        "color": "#000000",
                        "fontWeight": "bold"
                    }
                }
            }
        },
        "series": [{
            "name": value_col,
            "data": data_points,
            "colorByPoint": True
        }],
        "legend": {
            "enabled": True,
            "align": "center",
            "verticalAlign": "bottom",
            "layout": "horizontal"
        },
        "tooltip": {
            "pointFormat": "{series.name}: <b>{point.percentage:.1f}%</b>"
        }
    }
    
    print(f"DEBUG: Created pie chart config with {len(data_points)} data points")
    return chart_config


@skill(
    name="MI Data Explorer",
    description="A data explorer skill that returns Highcharts visualization format for MetricInsights integration. Extracts chart data and SQL queries.",
    capabilities="Generates SQL queries from natural language, returns Highcharts chart data in hchart format. Includes SQL query extraction for tooltips.",
    limitations="Only works within one dataset at a time. Returns chart data optimized for Highcharts visualization.",
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
def mi_data_explorer(parameters: SkillInput) -> SkillOutput:
    """
    MI Data Explorer skill

    Returns Highcharts visualization format for MetricInsights integration.
    Extracts chart data and SQL queries.
    """
    
    print("DEBUG: Starting MI Data Explorer skill")
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
        # Return error using chart format
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
        
        template_str = parameters.arguments.final_prompt_template
        template = Template(template_str)
        final_output = template.render(
            message=error_message,
            json_table=json.dumps(error_chart, indent=2)
        )
        
        return SkillOutput(
            final_prompt=final_output,
            narrative="Error occurred during data exploration",
            visualizations=result.visualizations if 'result' in locals() else [],
            export_data=result.export_data if 'result' in locals() else []
        )
    
    # Extract chart data from visualizations
    chart_data = None
    chart_title = ""
    chart_type = "LINE_CHART"  # Default
    user_question = parameters.arguments.user_chat_question_with_context
    
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
                                    result = find_highcharts_config(value, f"{path}.{key}")
                                    if result:
                                        return result
                            elif isinstance(obj, list):
                                for idx, item in enumerate(obj):
                                    result = find_highcharts_config(item, f"{path}[{idx}]")
                                    if result:
                                        return result
                            return None
                        
                        chart_config = find_highcharts_config(layout_data)
                        if chart_config:
                            # Validate chart data before using it
                            if is_chart_data_valid(chart_config):
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
                                
                                print(f"DEBUG: Extracted valid chart type: {chart_type}")
                                break
                            else:
                                print(f"DEBUG: Chart data validation failed for viz {i} - contains corrupted/placeholder data")
                                # Continue to next visualization or fall back to no chart
                        else:
                            print(f"DEBUG: No HighchartsChart found in layout {i}")
                            
                except json.JSONDecodeError:
                    print(f"DEBUG: Failed to parse layout as JSON for viz {i}")
                    continue
    else:
        print("DEBUG: No visualizations found")
    
    # Create message based on chart availability
    if chart_data:
        message = f"Generated chart visualization for: {user_question}"
        
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
        
        # Create Highcharts structure based on customer example
        chart_structure = {
            "data_type": "hchart",
            "data": {
                "chart_type": chart_type,
                "highChartsOptions": json.dumps(chart_data) if chart_data else "{}",
                "title": chart_title,
                "type": "chart"
            },
            "sql": sql_query
        }
        
    else:
        # No chart case - could be due to validation failure or no visualizations
        if hasattr(result, 'visualizations') and result.visualizations:
            # Try to create pie chart fallback from real data
            dataframe = None
            if result.export_data and len(result.export_data) > 0:
                first_export = result.export_data[0]
                dataframe = first_export.data
                print(f"DEBUG: Attempting pie chart fallback with DataFrame shape: {dataframe.shape}")
            
            # Check if this should be a pie chart and we have valid data
            if dataframe is not None and should_create_pie_chart(user_question, dataframe):
                print("DEBUG: Creating fallback pie chart from real data")
                pie_chart_config = create_pie_chart_config(dataframe, user_question)
                if pie_chart_config:
                    chart_data = pie_chart_config
                    chart_title = pie_chart_config.get('title', {}).get('text', f"Distribution for: {user_question}")
                    chart_type = "PIE_CHART"
                    message = f"Generated pie chart visualization for: {user_question}"
                    
                    # Continue with the normal flow (extract SQL, create structure)
                    sql_query = ""
                    if hasattr(result, 'final_prompt') and result.final_prompt:
                        final_prompt = result.final_prompt
                        import re
                        sql_patterns = [
                            r'"text":\s*"```sql\\n(.*?)\\n```"',
                            r'"text":\s*"```sql\\n(.*?)"',
                            r'(SELECT.*?LIMIT.*?)(?=")',
                            r'(SELECT.*?)(?=")',
                        ]
                        
                        for pattern in sql_patterns:
                            matches = re.findall(pattern, final_prompt, re.DOTALL | re.IGNORECASE)
                            if matches:
                                sql_query = matches[0].strip()
                                break
                    
                    print(f"DEBUG: Pie chart fallback successful with SQL: {sql_query[:100] if sql_query else 'None'}...")
                    
                    # Create chart structure and skip to the end
                    chart_structure = {
                        "data_type": "hchart",
                        "data": {
                            "chart_type": chart_type,
                            "highChartsOptions": json.dumps(chart_data) if chart_data else "{}",
                            "title": chart_title,
                            "type": "chart"
                        },
                        "sql": sql_query
                    }
                    
                    # Convert chart structure to JSON string
                    json_chart = json.dumps(chart_structure, indent=2)
                    
                    print(f"DEBUG: Created pie chart fallback response with:")
                    print(f"  - message length: {len(message)}")
                    print(f"  - chart type: {chart_structure['data']['chart_type']}")
                    print(f"  - has chart data: {bool(chart_data)}")
                    
                    # Use Jinja template for final output
                    template_str = parameters.arguments.final_prompt_template
                    template = Template(template_str)
                    final_output = template.render(
                        message=message,
                        json_table=json_chart
                    )
                    
                    return SkillOutput(
                        final_prompt=final_output,
                        narrative=message,
                        visualizations=result.visualizations,
                        export_data=result.export_data,
                        parameter_display_descriptions=result.parameter_display_descriptions if hasattr(result, 'parameter_display_descriptions') else [],
                        followup_questions=result.followup_questions if hasattr(result, 'followup_questions') else []
                    )
            
            # If pie chart fallback didn't work, fall back to invalid chart
            message = f"Chart visualization contained invalid data for: {user_question}. This may be due to insufficient data or visualization generation errors."
            chart_title = "Chart Data Validation Failed"
            chart_type = "INVALID_CHART"
        else:
            message = f"No chart visualization available for: {user_question}"
            chart_title = "No Chart Available"
            chart_type = "EMPTY_CHART"
        
        # Still extract SQL for debugging purposes
        sql_query = ""
        if hasattr(result, 'final_prompt') and result.final_prompt:
            final_prompt = result.final_prompt
            import re
            sql_patterns = [
                r'"text":\s*"```sql\\n(.*?)\\n```"',
                r'"text":\s*"```sql\\n(.*?)"',
                r'(SELECT.*?LIMIT.*?)(?=")',
                r'(SELECT.*?)(?=")',
            ]
            
            for pattern in sql_patterns:
                matches = re.findall(pattern, final_prompt, re.DOTALL | re.IGNORECASE)
                if matches:
                    sql_query = matches[0].strip()
                    break
        
        chart_structure = {
            "data_type": "hchart", 
            "data": {
                "chart_type": chart_type,
                "highChartsOptions": "{}",
                "title": chart_title,
                "type": "chart"
            },
            "sql": sql_query
        }
    
    # Convert chart structure to JSON string
    json_chart = json.dumps(chart_structure, indent=2)
    
    print(f"DEBUG: Created chart response with:")
    print(f"  - message length: {len(message)}")
    print(f"  - chart type: {chart_structure['data']['chart_type']}")
    print(f"  - has chart data: {bool(chart_data)}")
    
    # Use Jinja template for final output
    template_str = parameters.arguments.final_prompt_template
    template = Template(template_str)
    final_output = template.render(
        message=message,
        json_table=json_chart  # Keep same variable name for template compatibility
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