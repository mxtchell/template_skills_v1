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


def format_number(value, is_currency=False):
    """
    Format numbers with proper formatting - commas, rounding, currency prefix
    """
    if not isinstance(value, (int, float)):
        return value
    
    # Round large numbers to whole digits
    if abs(value) >= 1000:
        formatted_value = f"{value:,.0f}"
    else:
        formatted_value = f"{value:,.2f}"
    
    # Add currency prefix for sales values
    if is_currency:
        formatted_value = f"${formatted_value}"
    
    return formatted_value


def format_table_data(table_data, columns):
    """
    Format table data with proper number formatting
    """
    formatted_data = []
    for row in table_data:
        formatted_row = {}
        for key, value in row.items():
            # Check if this looks like a currency field
            is_currency_field = any(currency_word in key.lower() for currency_word in ['sales', 'revenue', 'price', 'cost', 'amount'])
            
            if isinstance(value, (int, float)):
                formatted_row[key] = format_number(value, is_currency_field)
            else:
                formatted_row[key] = value
        formatted_data.append(formatted_row)
    
    return formatted_data


def enhance_chart_formatting(chart_config):
    """
    Enhance Highcharts configuration with better number and date formatting
    """
    if not chart_config or not isinstance(chart_config, dict):
        return chart_config
    
    # Add tooltip formatting for better number display
    if 'tooltip' not in chart_config:
        chart_config['tooltip'] = {}
    
    # Enhanced tooltip formatter with currency detection
    chart_config['tooltip']['formatter'] = """
        function() {
            var value = this.y;
            var formattedValue;
            
            // Format large numbers with commas and rounding
            if (Math.abs(value) >= 1000) {
                formattedValue = Highcharts.numberFormat(value, 0);
            } else {
                formattedValue = Highcharts.numberFormat(value, 2);
            }
            
            // Add currency prefix for sales/revenue values
            var seriesName = this.series.name || '';
            if (seriesName.toLowerCase().indexOf('sales') >= 0 || 
                seriesName.toLowerCase().indexOf('revenue') >= 0 ||
                seriesName.toLowerCase().indexOf('price') >= 0 ||
                seriesName.toLowerCase().indexOf('cost') >= 0) {
                formattedValue = '$' + formattedValue;
            }
            
            var pointName = this.point.name || this.x;
            if (this.point.category !== undefined) {
                pointName = this.point.category;
            }
            
            // Format dates in tooltip if it's a datetime axis
            if (this.x && typeof this.x === 'number' && this.x > 1000000000000) {
                pointName = Highcharts.dateFormat('%B %Y', this.x);
            }
            
            return '<b>' + pointName + '</b><br/>' + 
                   this.series.name + ': <b>' + formattedValue + '</b>';
        }
    """.strip()
    
    # Format yAxis labels for currency and large numbers
    if 'yAxis' in chart_config:
        yaxis = chart_config['yAxis']
        if isinstance(yaxis, dict):
            if 'labels' not in yaxis:
                yaxis['labels'] = {}
            
            # Check if this appears to be a currency axis
            title_text = yaxis.get('title', {}).get('text', '').lower()
            is_currency_axis = any(word in title_text for word in ['sales', 'revenue', 'price', 'cost'])
            
            if is_currency_axis:
                yaxis['labels']['formatter'] = """
                    function() {
                        if (Math.abs(this.value) >= 1000) {
                            return '$' + Highcharts.numberFormat(this.value, 0);
                        } else {
                            return '$' + Highcharts.numberFormat(this.value, 2);
                        }
                    }
                """.strip()
            else:
                yaxis['labels']['formatter'] = """
                    function() {
                        if (Math.abs(this.value) >= 1000) {
                            return Highcharts.numberFormat(this.value, 0);
                        } else {
                            return Highcharts.numberFormat(this.value, 2);
                        }
                    }
                """.strip()
    
    # Format xAxis for better date display
    if 'xAxis' in chart_config:
        xaxis = chart_config['xAxis']
        if isinstance(xaxis, dict) and xaxis.get('type') == 'datetime':
            if 'labels' not in xaxis:
                xaxis['labels'] = {}
            xaxis['labels']['formatter'] = """
                function() {
                    return Highcharts.dateFormat('%b %Y', this.value);
                }
            """.strip()
    
    return chart_config


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
                            # Validate chart data before using it
                            if is_chart_data_valid(chart_config):
                                # Enhance chart formatting before using it
                                enhanced_chart_config = enhance_chart_formatting(chart_config.copy())
                                
                                # Use the enhanced chart data from visualization service
                                chart_data = enhanced_chart_config
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
                                
                                print(f"DEBUG: Extracted and enhanced valid chart type: {chart_type}")
                                break
                            else:
                                print(f"DEBUG: Chart data validation failed for viz {i} - contains corrupted/placeholder data")
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
        raw_table_data = dataframe.to_dict('records')
        
        # Create columns metadata
        columns = []
        for col in dataframe.columns:
            columns.append({
                "key": col,
                "label": col.replace('_', ' ').title()
            })
        
        # Format the table data with proper number formatting
        formatted_table_data = format_table_data(raw_table_data, columns)
        
        table_structure = {
            "data_type": "table",
            "data": formatted_table_data,
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
                    
                    # Look specifically for Markdown elements with SQL
                    def find_sql_in_layout(obj, path=""):
                        if isinstance(obj, dict):
                            # Check if this is a Markdown element with SQL
                            if obj.get('type') == 'Markdown' and 'text' in obj:
                                text = obj['text']
                                if '```sql' in text:
                                    print(f"DEBUG: Found Markdown with SQL at {path}")
                                    print(f"DEBUG: Markdown text preview: {repr(text[:200])}...")
                                    # Extract SQL from markdown block - try multiple patterns
                                    import re
                                    patterns = [
                                        r'```sql\s*\n(.*?)```',   # Standard format - removed \n before ```
                                        r'```sql\n(.*?)```',      # No extra spaces - removed \n before ```
                                        r'```sql\s*(.*?)```',     # With optional spaces
                                        r'```sql(.*?)```',        # Any content between
                                        r'```sql\s*\n(.*)',       # Everything after ```sql\n (no closing required)
                                        r'```sql\n(.*)',          # Everything after ```sql\n (no closing required)
                                    ]
                                    
                                    for pattern in patterns:
                                        match = re.search(pattern, text, re.DOTALL)
                                        if match:
                                            sql_result = match.group(1).strip()
                                            print(f"DEBUG: SQL extraction successful with pattern: {pattern}")
                                            print(f"DEBUG: Extracted SQL preview: {sql_result[:100]}...")
                                            return sql_result
                                    
                                    print(f"DEBUG: No SQL extraction patterns matched")
                                    return None
                            
                            # Check for text fields with SQL
                            elif 'text' in obj and isinstance(obj['text'], str):
                                text = obj['text']
                                if '```sql' in text:
                                    print(f"DEBUG: Found text with SQL at {path}")
                                    import re
                                    match = re.search(r'```sql\s*\n(.*?)\n```', text, re.DOTALL)
                                    if match:
                                        return match.group(1).strip()
                            
                            # Recursively search other objects
                            for key, value in obj.items():
                                result_sql = find_sql_in_layout(value, f"{path}.{key}")
                                if result_sql:
                                    return result_sql
                                    
                        elif isinstance(obj, list):
                            for idx, item in enumerate(obj):
                                result_sql = find_sql_in_layout(item, f"{path}[{idx}]")
                                if result_sql:
                                    return result_sql
                        return None
                    
                    sql_query = find_sql_in_layout(layout_data)
                    if sql_query:
                        print(f"DEBUG: Successfully extracted SQL from layout: {sql_query[:100]}...")
                        break
                        
                except json.JSONDecodeError:
                    print(f"DEBUG: Failed to parse layout JSON for viz {i}")
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