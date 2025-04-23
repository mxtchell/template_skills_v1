from dataclasses import dataclass
from datetime import datetime
import json
import os
from typing import Any, Dict, List, Union
from answer_rocket import AnswerRocketClient
import pandas as pd
from skill_framework import ExitFromSkillException, SkillVisualization, preview_skill, skill, SkillParameter, SkillInput, SkillOutput
import numpy as np
import jinja2
from skill_framework.layouts import wire_layout

from data_explorer_config import FINAL_PROMPT_TEMPLATE, DATA_EXPLORE_LAYOUT, SQL_ERROR_FINAL_PROMPT_TEMPLATE, SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT

import sqlparse

def format_sql(query: str) -> str:
    """
    Takes a raw SQL query string and returns a formatted version
    with proper indentation and newlines.
    """
    # reindent=True will handle the newlines and indentation
    # keyword_case='upper' will capitalize all SQL keywords
    formatted_query = sqlparse.format(
        query,
        reindent=True,
        keyword_case='upper'
    )
    return formatted_query


@dataclass
class MostRecentPayloadResult:
    question_is_follow_up: bool
    question: str | None = None
    formatted_df: pd.DataFrame | None = None
    unformatted_df: pd.DataFrame | None = None
    sql: str | None = None
    explanation: str | None = None
    visualization: str | None = None
    df_truncated: bool | None = None
    df_string: str | None = None
    column_metadata_map: dict | None = None
@dataclass
class VisResult:
    success: bool
    visualization: dict | None = None
    error: str | None = None

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
            name="question_is_follow_up",
            description="Set this field to true if the user is asking for modifications to the generated visualization of the previous question. If the user's question implies using/retrieving different data than the last question or is a follow-up question that doesn't involve the visualization, set this field to false.",
            required=True,
            constrained_values=["true", "false"]
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

def DataExplorer(parameters: SkillInput) -> SkillOutput:
    """
    Data Explorer skill

    This skill is able to generate SQL queries from a natural language user query, return the retrieved data and the SQL query used to retrieve it, and attempt to generate a visualization from the retrieved data.

    """
    try:
        run_local = False
        print("Starting DataExplorer")
        print("Parameters: " + str(parameters.arguments))

        sql_row_limit_exceeded = False
        user_query = parameters.arguments.user_chat_question_with_context
        question_is_follow_up = parameters.arguments.question_is_follow_up == "true"

        data_explore_layout = json.loads(parameters.arguments.data_explore_vis_layout)
        data_explore_layout_variables = {
            "error_hidden": True,
            "error_message": None,
            "visualization_hidden": True,
            "visualization": None,
            "truncate_message_hidden": True,
            "truncate_message_text": None,
            "data_table_hidden": True,
            "data_table_columns": None,
            "data_table_data": None,
            "sql_hidden": True,
            "sql_text": None,
            "sql_explanation": None
        }

        success_but_empty = False

        arc = AnswerRocketClient()
        print("SDK Connected OK" if arc.can_connect() else "SDK Connection Failed")

        skill_memory_payload = {"seq_timestamp": datetime.now().isoformat(),
                                "formatted_df": None,
                                "unformatted_df": None,
                                "sql": None,
                                "explanation": None,
                                "question": user_query,
                                "visualization": None}
        

        follow_up_flag = False
        if question_is_follow_up:
            print("Attempting to retrieve skill memory payload...")
            arc.skill.update_loading_message("Attempting to retrieve skill memory payload...")
            retrieved_payload = retrieve_last_payload(arc)
            follow_up_flag = retrieved_payload.question_is_follow_up
            if follow_up_flag:
                formatted_df = pd.DataFrame.from_dict(retrieved_payload.formatted_df)
                unformatted_df = pd.DataFrame.from_dict(retrieved_payload.unformatted_df)
                data_table_columns = [{"name": col} for col in formatted_df.columns]
                data_table_data = formatted_df.to_numpy().tolist()
                df_truncated = retrieved_payload.df_truncated
                df_string = retrieved_payload.df_string
                skill_memory_payload.update({
                    "formatted_df": formatted_df.to_dict(orient="records"),
                    "unformatted_df": unformatted_df.to_dict(orient="records"),
                    "sql": retrieved_payload.sql,
                    "explanation": retrieved_payload.explanation,
                    "prev_question": retrieved_payload.question,
                    "prev_visualization": retrieved_payload.visualization,
                    "df_truncated": df_truncated,
                    "df_string": df_string
                })
                update_skill_memory_payload(skill_memory_payload, arc)
        
        update_skill_memory_payload(skill_memory_payload, arc)
        if not follow_up_flag:
            print("Generating SQL...")
            arc.skill.update_loading_message("Generating SQL...")
            if run_local:
                dataset_id = os.getenv("DATASET_ID")
            else:
                dataset_id = arc.config.get_copilot_skill().dataset_id
            sql_res = arc.data.run_sql_ai(
                dataset_id=dataset_id, 
                question=user_query
            )
            if sql_res is None:
                print("SQLGenAi Service failed to return a response")
                # sql was not generated, surface error in output block
                skill_memory_payload.update({
                    "error": "SQLGenAi Service failed to return a response",
                })
                update_skill_memory_payload(skill_memory_payload, arc)
                raise ExitFromSkillException(message=skill_memory_payload.get("error", None), prompt_message="Let the user know that an error occurred, suggest that the user should try another question")
            if sql_res.success:
                print("SQLGenAi Service returned a response")
                data = sql_res.data
                columns = [col['name'] for col in data.get('columns', [])]
                rows = [row.get('data', []) for row in data.get('rows', [])]
                df = pd.DataFrame(rows, columns=columns)
                if df.shape[0] == 10000:
                    sql_row_limit_exceeded = True
                df = df.fillna(0)
                print("df empty: " + str(df.empty))
                if df.empty:
                    success_but_empty = True
            if sql_res.success and not success_but_empty:
                print("sql_res.sql: " + str(sql_res.sql))
                print("sql_res.explanation: " + str(sql_res.explanation))
                print("sql_res.title: " + str(sql_res.title))
                print("sql_res.column_metadata_map: " + str(sql_res.column_metadata_map))
                column_metadata_map = sql_res.column_metadata_map
                # rename columns based on column metadata map
                for col in df.columns:
                    if col in column_metadata_map:
                        df = df.rename(columns={col: column_metadata_map[col].get("display_name", col)})
                        column_metadata_map[column_metadata_map[col].get("display_name", col)] = column_metadata_map[col]

                unformatted_df = df.copy()
                unformatted_df = unformatted_df.applymap(lambda x: round(x, 2) if isinstance(x, (int, float)) and x > 1 else x)
                

                formatted_df = df.copy()
                # apply formatting rules from sql_res.column_metadata_map[col].format_string (an example value for this is "%.2f")
                for col in df.columns:
                    if col in column_metadata_map:
                        format_string = column_metadata_map[col].get("format_string", None)
                        if format_string:
                            try:
                                formatted_df[col] = formatted_df[col].apply(
                                    lambda x: format(x, format_string) if isinstance(x, (int, float)) else x
                                )
                            except Exception as e:
                                print(f"Error formatting column '{col}' with format string '{format_string}': {e}")
                
                if formatted_df.shape[0] > 100:
                    formatted_df = formatted_df.head(100)
                    df_string = formatted_df.to_string(index=False)
                    df_truncated = True
                    data_explore_layout_variables.update({
                        "truncate_message_hidden": False
                    })
                else:
                    df_string = formatted_df.to_string(index=False)
                    df_truncated = False
                skill_memory_payload.update({
                    "formatted_df": formatted_df.to_dict(orient="records"),
                    "unformatted_df": unformatted_df.to_dict(orient="records"),
                    "sql": json.dumps(sql_res.sql)[1:-1],
                    "explanation": sql_res.explanation.replace("\\n", "\n"),
                    "title": sql_res.title,
                    "df_truncated": df_truncated,
                    "df_string": df_string,
                    "column_metadata_map": column_metadata_map
                })
                update_skill_memory_payload(skill_memory_payload, arc)
                data_table_columns = [{"name": col} for col in formatted_df.columns]
                data_table_data = formatted_df.to_numpy().tolist()
            else:
                if sql_res.sql is not None:
                    print("SQLGenAi Service returned an error (or df is empty), but sql was generated")
                    skill_memory_payload.update({
                        "sql": json.dumps(sql_res.sql)[1:-1],
                        "explanation": sql_res.explanation.replace("\\n", "\n"),
                        "title": sql_res.title,
                        "error": sql_res.error
                    })
                    update_skill_memory_payload(skill_memory_payload, arc)
                    data_explore_layout_variables.update({
                        "error_hidden": True if success_but_empty else False,
                        "error_message": skill_memory_payload.get("error", None),
                        "sql_hidden": False,
                        "sql_text": "```sql\n" + format_sql(skill_memory_payload.get("sql", None)),
                        "sql_explanation": skill_memory_payload.get("explanation", None),
                        "truncate_message_hidden": False if success_but_empty else True,
                        "truncate_message_text": "The query ran successfully, but no data was returned" if success_but_empty else "The SQL query reached the 10,000 row default limit. The data has also been truncated to 100 rows for display purposes." if sql_row_limit_exceeded else None
                    })
                    rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
                    # sql was generated but there was an error, surface error in output block and display sql
                    #format output block
                    if success_but_empty:
                        final_prompt = parameters.arguments.sql_success_empty_data_final_prompt
                    else:
                        final_prompt = jinja2.Template(parameters.arguments.sql_error_final_prompt_template).render(error_message=skill_memory_payload.get("error", ""))

                    skill_output = SkillOutput(
                        final_prompt=final_prompt,
                        narrative="",
                        visualizations=[SkillVisualization(
                            title=skill_memory_payload.get("title", "Data Explore Skill"),
                            layout = rendered_data_explore_layout
                        )]
                    )
                    return skill_output
                else:
                    print("SQLGenAi Service returned an error, and no sql was generated")
                    skill_memory_payload.update({
                        "error": sql_res.error
                    })
                    update_skill_memory_payload(skill_memory_payload, arc)
                    raise ExitFromSkillException(message=skill_memory_payload.get("error", None), prompt_message="Let the user know that an error occurred, suggest that the user should try another question")
        
        arc.skill.update_loading_message("Generating visualization...")
        print("Generating visualization...")
        vis_result = generate_visualization(user_query, pd.DataFrame.from_dict(skill_memory_payload.get("unformatted_df")), arc, follow_up_flag, skill_memory_payload.get("prev_question", None), skill_memory_payload.get("prev_visualization", None))
        if vis_result.success:
            print("Successfully generated visualization")
            skill_memory_payload.update({
                "visualization": json.dumps(vis_result.visualization),
            })
            update_skill_memory_payload(skill_memory_payload, arc)
        else:
            print("Failed to generate visualization, still display sql + data etc")
            print("vis_result.error: " + str(vis_result.error))
            # failed to generate visualization, surface error in output block, still display sql + data etc
            skill_memory_payload.update({
                "error": vis_result.error,
            })
            update_skill_memory_payload(skill_memory_payload, arc)
            
            data_explore_layout_variables.update({
                "error_hidden": True,
                "data_table_hidden": False,
                "data_table_columns": data_table_columns,
                "data_table_data": data_table_data,
                "sql_hidden": False,
                "sql_text": "```sql\n" + format_sql(skill_memory_payload.get("sql", None)),
                "sql_explanation": skill_memory_payload.get("explanation", None)
            })
            vis_type = None
            rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
            skill_output = SkillOutput(
                final_prompt=jinja2.Template(parameters.arguments.final_prompt_template).render(sql=skill_memory_payload.get("sql", None), df_string=df_string, df_truncated=df_truncated, vis_type=vis_type),
                narrative="",
                visualizations=[SkillVisualization(
                    title=skill_memory_payload.get("title", "Data Explore Skill"),
                    layout = rendered_data_explore_layout
                )]
            )
            return skill_output
        
        #TODO : test harness needs to be updated for new df handling
        test_run_input_payload = {
            "user_query": user_query,
            "df": formatted_df.to_dict(orient="records"),
            "vis_json": vis_result.visualization,
            "prev_query": skill_memory_payload.get("prev_question", None),
            "prev_vis_json": skill_memory_payload.get("prev_visualization", None)
        }
        print("(*)"*50)
        print("PRINTING TEST RUN INPUT PAYLOAD")
        print(json.dumps(test_run_input_payload, indent=4))
        print("(*)"*50)
        print()


        data_explore_layout_variables.update({
            "visualization_hidden": False,
            "visualization": vis_result.visualization["options"],
            "data_table_hidden": False,
            "data_table_columns": data_table_columns,
            "data_table_data": data_table_data,
            "sql_hidden": False,
            "sql_text": "```sql\n" + format_sql(skill_memory_payload.get("sql", None)),
            "sql_explanation": skill_memory_payload.get("explanation", None)
        })
        rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
        print("FINAL RENDERED ARTIFACT PANEL")
        print(rendered_data_explore_layout)
        print("(*)"*50)

        
        
        
        vis_type = json.loads(skill_memory_payload.get("visualization", None)).get("options", {}).get("chart", {}).get("type", None)

        skill_output = SkillOutput(
            final_prompt=jinja2.Template(parameters.arguments.final_prompt_template).render(sql=skill_memory_payload.get("sql", None), df_string=df_string, df_truncated=df_truncated, vis_type=vis_type),
            narrative="",
            visualizations=[SkillVisualization(
                title=skill_memory_payload.get("title", "Data Explore Skill"),
                layout = rendered_data_explore_layout
            )]
        )
        return skill_output
        
    except Exception as e:
        if isinstance(e, ExitFromSkillException):
            raise e
        # unexpected error case
        print("Unexpected error encountered in DataExplorer: " + str(e))
        raise ExitFromSkillException(message=str(e), prompt_message="Let the user know that an unexpected error occurred, suggest that the user should try another question")


def retrieve_last_payload(arc: AnswerRocketClient) -> MostRecentPayloadResult:
    """
    Retrieve the most recent skill memory payload from the chat thread.
    """
    entries = arc.chat.get_chat_thread(arc._client_config.thread_id).entries
    most_recent_payload = None
    if len(entries) > 0:
        most_recent_timestamp = datetime.min
        for entry in entries:
            try:
                print("checking entry for skill memory payload")
                if hasattr(entry, 'skill_memory_payload') and entry.skill_memory_payload is not None:
                    payload = entry.skill_memory_payload
                    print("retrieved payload: " + str(payload))
                    if payload.get("seq_timestamp", None) is not None:
                        timestamp = payload.get("seq_timestamp", None)
                        print("Retrieved timestamp: " + str(timestamp))
                        if isinstance(timestamp, str):
                            try:
                                print("Converting timestamp to datetime object")
                                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            except ValueError as e:
                                print("Warning: Failed to convert timestamp to datetime object, using original value -- error: " + str(e))
                                #remove this if we aren't running into this error
                                pass
                        print("Comparing timestamp: " + str(timestamp) + " with most recent timestamp: " + str(most_recent_timestamp))
                        if timestamp > most_recent_timestamp:
                            most_recent_timestamp = timestamp
                            most_recent_payload = payload
                            print(f"Found payload with timestamp: {most_recent_timestamp}")
            except Exception as e:
                print(f"unexpected error retrieving skill memory payload: {str(e)}")
                raise e
        if most_recent_payload is not None:
            prev_formatted_df = most_recent_payload.get("formatted_df", None)
            prev_unformatted_df = most_recent_payload.get("unformatted_df", None)
            prev_vis_json = most_recent_payload.get("visualization", None)
            if prev_formatted_df is not None:
                if prev_vis_json is None:
                    prev_vis_json = "Previous visualization failed to generate with given data and question, do your best to generate a successful visualization"
                return MostRecentPayloadResult(question_is_follow_up=True, question=most_recent_payload.get("question", None), 
                                               formatted_df=prev_formatted_df, unformatted_df=prev_unformatted_df, sql=most_recent_payload.get("sql", None), explanation=most_recent_payload.get("explanation", None), 
                                               visualization=prev_vis_json, column_metadata_map=most_recent_payload.get("column_metadata_map", None))
            else:
                print("No previous data found, setting followup to false")
                return MostRecentPayloadResult(question_is_follow_up=False)
        else:
            print("No skill memory payload found in thread, setting followup to false")
            return MostRecentPayloadResult(question_is_follow_up=False)
    else:
        print("No entries found for thread, setting followup to false")
        return MostRecentPayloadResult(question_is_follow_up=False)
    

def convert_column_to_json_serializable(col: pd.Series) -> List[Any]:
    """
    Convert a pandas Series into a list of JSON serializable values.

    - Numeric columns remain as numbers.
    - Datetime columns are converted to ISO format strings.
    - Strings that represent formatted numbers (e.g., "$1,000K") are converted to floats.

    Args:
        col: A pandas Series representing a dataframe column.
    
    Returns:
        A list of JSON serializable values.
    """
    if pd.api.types.is_numeric_dtype(col):
        result = []
        for val in col:
            if pd.isna(val):
                print(f"Dropping NaN value in numeric column: {col.name}")
                result.append(None)
            else:
                result.append(val)
        return result
    elif pd.api.types.is_datetime64_any_dtype(col):
        result = []
        for val in col:
            if pd.isna(val):
                print(f"Dropping NaN value in datetime column: {col.name}")
                result.append(None)
            else:
                result.append(val.strftime("%Y-%m-%dT%H:%M:%S"))
        return result
    else:
        def safe_convert(x: Any) -> Any:
            if pd.isna(x):
                print(f"Dropping NaN value in column: {col.name}")
                return None  # Replace NaN with None, which converts to null in JSON
            if isinstance(x, pd.Timestamp):
                return x.isoformat()
            if isinstance(x, np.generic):
                return x.item()
            if isinstance(x, str):
                s = x.strip()
                if s.startswith("$"):
                    s = s[1:]
                s = s.replace(",", "")
                if s and s[-1] in "MmKkBb":
                    s = s[:-1]
                try:
                    return float(s)
                except ValueError:
                    return x
            return x

    return [safe_convert(x) for x in col.tolist()]


def hydrate_highcharts_json(ui_json: Union[Dict[str, Any], List[Any]], df: pd.DataFrame, breakout: str = None, pie: bool = False) -> Union[Dict[str, Any], List[Any]]:
    """
    Replace references to column names in Highcharts JSON with actual data from the given dataframe.

    - For xAxis.categories, if the value is a string and a valid column name, the column's string values are used.
    - For series[].data, if the value is a string and a valid column name, the column data is converted to JSON serializable values and used.
    - if the series has a breakout field, modify the series data to be multiple series, one for each unique value in the breakout column.
    
    If a referenced column does not exist, the field is replaced with an empty list.
    
    Args:
        ui_json: The Highcharts JSON object (can be a dict or list).
        df: A pandas DataFrame containing the data.
        breakout: The column to breakout the data by. If None, the data is not broken out.
    Returns:
        The hydrated Highcharts JSON object.
    """
    if isinstance(ui_json, dict):
        categories_field = None
        if "xAxis" in ui_json and isinstance(ui_json["xAxis"], dict) and not pie:
            categories_field = ui_json["xAxis"].get("categories")
            if isinstance(categories_field, str) and categories_field in df.columns:
                if breakout and breakout in df.columns:
                    ui_json["xAxis"]["categories"] = df[categories_field].unique().astype(str).tolist()
                else:
                    ui_json["xAxis"]["categories"] = df[categories_field].astype(str).tolist()
            else:
                ui_json["xAxis"]["categories"] = []

        series_list = ui_json.get("series")
        if isinstance(series_list, list):
            if breakout and breakout in df.columns:
                original_series = series_list.copy()
                if "series" in ui_json:
                    ui_json["series"] = []
                
                for breakout_value in df[breakout].unique():
                    filtered_df = df[df[breakout] == breakout_value]
                    
                    for series in original_series:
                        data_field = series.get("data")
                        if isinstance(data_field, str) and data_field in filtered_df.columns:
                            new_series = series.copy()
                            new_series["name"] = f"{breakout_value}"
                            new_series["data"] = convert_column_to_json_serializable(filtered_df[data_field])
                            ui_json["series"].append(new_series)
            else:
                for series in series_list:
                    if not pie:
                        data_field = series.get("data")
                        if isinstance(data_field, str) and data_field in df.columns:
                            series["data"] = convert_column_to_json_serializable(df[data_field])
                        else:
                            series["data"] = []
                    else:
                        series["data"] = []
                        value_column = series.get("valueColumn")
                        label_column = series.get("labelColumn")
                        if value_column and label_column and value_column in df.columns and label_column in df.columns:
                            series["data"] = [{"name": label, "y": value} for label, value in zip(df[label_column], convert_column_to_json_serializable(df[value_column]))]
                            series["colorByPoint"] = True
                            series["name"] = value_column
        # Recursively process any nested dictionaries or lists.
        for key, value in ui_json.items():
            if isinstance(value, (dict, list)):
                ui_json[key] = hydrate_highcharts_json(value, df, breakout=breakout, pie=pie)

    elif isinstance(ui_json, list):
        ui_json = [hydrate_highcharts_json(item, df, breakout=breakout, pie=pie) for item in ui_json]

    return ui_json


def generate_visualization(user_query: str, df: pd.DataFrame, arc: AnswerRocketClient, question_is_follow_up: bool = False, previous_question: str = None, previous_vis_json: str = None, model_override: str = None) -> VisResult:
    """
    Receive as input a dictionary of data and an AnswerRocketClient object.

    Generates a visualization for the provided data in the form of a dynamic layout highcharts object.

    Returns a dictionary with the status and message keys. On success, the message contains the visualization JSON.
    """

    
    try:
        print("df type: " + str(type(df)))
        column_options_string = "[" + ", ".join(df.columns) + "]"
        highcharts_schema = f"""
const highchartsSchema = {{
type: 'object',
title: 'Highcharts Configuration',
properties: {{
    options: {{
    type: 'object',
    title: 'Chart Configuration',
    properties: {{
        chart: {{
        type: 'object',
        title: 'Chart Options',
        properties: {{
            type: {{
            type: 'string',
            title: 'Chart Type',
            enum: ['column', 'line', 'spline', 'area', 'areaspline', 'bar', 'pie', 'scatter'],
            enumNames: [
                'Column',
                'Line',
                'Smooth Line',
                'Area',
                'Smooth Area',
                'Bar',
                'Pie',
                'Scatter',
            ],
            }},
            polar: {{
            type: 'boolean',
            title: 'Polar Chart',
            }},
            alignTicks: {{
            type: 'boolean',
            title: 'Align Ticks',
            }},
            height: {{
            type: 'number',
            title: 'Chart Height',
            description: 'Height of the chart in pixels',
            }},
        }},
        }},
        title: {{
        type: 'object',
        title: 'Title Options',
        properties: {{
            text: {{
            type: 'string',
            title: 'Chart Title',
            }},
            style: {{
            type: 'object',
            title: 'Title Style',
            properties: {{
                fontSize: {{
                type: 'string',
                title: 'Font Size',
                }},
                fontWeight: {{
                type: 'string',
                title: 'Font Weight',
                enum: ['normal', 'bold', 'lighter', 'bolder'],
                }},
            }},
            }},
        }},
        }},
        xAxis: {{
        type: 'object',
        title: 'X Axis',
        properties: {{
            categories: {{
            type: 'string',
            title: 'Categories', // select from the provided column options and the data will be populated programmatically
            enum: {column_options_string},
            }},
            title: {{
            type: 'object',
            title: 'Axis Title',
            properties: {{
                text: {{
                type: 'string',
                title: 'Text',
                }},
            }},
            }},
        }},
        }},
        yAxis: {{
        type: 'object',
        title: 'Y Axis',
        properties: {{
            min: {{
            type: 'number',
            title: 'Minimum Value',
            }},
            max: {{
            type: 'number',
            title: 'Maximum Value',
            }},
            title: {{
            type: 'object',
            title: 'Axis Title',
            properties: {{
                text: {{
                type: 'string',
                title: 'Text',
                }},
            }},
            }},
            plotLines: {{
            type: 'array',
            title: 'Plot Lines',
            items: {{
                type: 'object',
                properties: {{
                color: {{
                    type: 'string',
                    format: 'color',
                    title: 'Line Color',
                }},
                width: {{
                    type: 'number',
                    title: 'Line Width',
                }},
                value: {{
                    type: 'number',
                    title: 'Line Value',
                }},
                zIndex: {{
                    type: 'number',
                    title: 'Z-Index',
                }},
                label: {{
                    type: 'object',
                    title: 'Line Label',
                    properties: {{
                    text: {{
                        type: 'string',
                        title: 'Label Text',
                    }},
                    align: {{
                        type: 'string',
                        title: 'Label Alignment',
                        enum: ['left', 'center', 'right'],
                    }},
                    style: {{
                        type: 'object',
                        title: 'Label Style',
                        properties: {{
                        color: {{
                            type: 'string',
                            format: 'color',
                            title: 'Label Color',
                        }},
                        }},
                    }},
                    }},
                }},
                dashStyle: {{
                    type: 'string',
                    title: 'Line Style',
                    enum: [
                    'solid',
                    'shortdash',
                    'shortdot',
                    'shortdashdot',
                    'shortdashdotdot',
                    'dot',
                    'dash',
                    'longdash',
                    'dashdot',
                    'longdashdot',
                    'longdashdotdot',
                    ],
                }},
                }},
            }},
            }},
        }},
        }},
        plotOptions: {{
        type: 'object',
        title: 'Plot Options',
        properties: {{
            column: {{
            type: 'object',
            title: 'Column Options',
            properties: {{
                stacking: {{
                type: ['string', 'null'],
                title: 'Stacking Type',
                enum: ['normal', 'percent', null],
                enumNames: ['Normal', 'Percentage', 'None'],
                }},
                dataLabels: {{
                type: 'object',
                title: 'Data Labels',
                properties: {{
                    enabled: {{
                    type: 'boolean',
                    title: 'Enable Data Labels',
                    }},
                    formatter: {{
                    type: 'string',
                    title: 'Label Formatter',
                    description: 'JavaScript function as string to format the data label',
                    }},
                    inside: {{
                    type: 'boolean',
                    title: 'Place Labels Inside',
                    }},
                    align: {{
                    type: 'string',
                    title: 'Label Alignment',
                    enum: ['left', 'center', 'right'],
                    }},
                    color: {{
                    type: 'string',
                    format: 'color',
                    title: 'Label Color',
                    }},
                    style: {{
                    type: 'object',
                    title: 'Label Style',
                    properties: {{
                        fontSize: {{
                        type: 'string',
                        title: 'Font Size',
                        }},
                    }},
                    }},
                }},
                }},
            }},
            }},
        }},
        }},
        series: {{
        type: 'array',
        title: 'Data Series',
        items: {{
            type: 'object',
            properties: {{
            name: {{
                type: 'string',
                title: 'Series Name',
            }},
            data: {{
                type: 'string',
                title: 'Data Points', // select from the provided column options and the data will be populated programmatically
                enum: {column_options_string},
            }},
            color: {{
                type: 'string',
                format: 'color',
                title: 'Series Color',
            }},
            }},
        }},
        }},
        legend: {{
        type: 'object',
        title: 'Legend Options',
        properties: {{
            enabled: {{
            type: 'boolean',
            title: 'Show Legend',
            }},
            align: {{
            type: 'string',
            title: 'Horizontal Alignment',
            enum: ['left', 'center', 'right'],
            enumNames: ['Left', 'Center', 'Right'],
            }},
            verticalAlign: {{
            type: 'string',
            title: 'Vertical Alignment',
            enum: ['top', 'middle', 'bottom'],
            enumNames: ['Top', 'Middle', 'Bottom'],
            }},
            layout: {{
            type: 'string',
            title: 'Layout Direction',
            enum: ['horizontal', 'vertical'],
            enumNames: ['Horizontal', 'Vertical'],
            }},
        }},
        }},
    }},
    }},
}},
}}
        """
        
        highcharts_schema = f"""
const highchartsSchema = {{
type: 'object',
title: 'Highcharts Configuration',
properties: {{
    options: {{
    type: 'object',
    title: 'Chart Configuration',
    properties: {{
        chart: {{
        type: 'object',
        title: 'Chart Options',
        properties: {{
            type: {{
            type: 'string',
            title: 'Chart Type',
            enum: ['column', 'line', 'spline', 'area', 'areaspline', 'bar', 'pie', 'scatter'],
            enumNames: [
                'Column',
                'Line',
                'Smooth Line',
                'Area',
                'Smooth Area',
                'Bar',
                'Pie',
                'Scatter',
            ],
            }},
            polar: {{
            type: 'boolean',
            title: 'Polar Chart',
            }},
            alignTicks: {{
            type: 'boolean',
            title: 'Align Ticks',
            }},
            height: {{
            type: 'number',
            title: 'Chart Height',
            description: 'Height of the chart in pixels',
            }},
        }},
        }},
        title: {{
        type: 'object',
        title: 'Title Options',
        properties: {{
            text: {{
            type: 'string',
            title: 'Chart Title',
            }},
            style: {{
            type: 'object',
            title: 'Title Style',
            properties: {{
                fontSize: {{
                type: 'string',
                title: 'Font Size',
                }},
                fontWeight: {{
                type: 'string',
                title: 'Font Weight',
                enum: ['normal', 'bold', 'lighter', 'bolder'],
                }},
            }},
            }},
        }},
        }},
        xAxis: {{
        type: 'object',
        title: 'X Axis',
        properties: {{
            categories: {{
            type: 'string',
            title: 'Categories', // select from the provided column options and the data will be populated programmatically
            enum: {column_options_string},
            }},
            title: {{
            type: 'object',
            title: 'Axis Title',
            properties: {{
                text: {{
                type: 'string',
                title: 'Text',
                }},
            }},
            }},
        }},
        }},
        yAxis: {{
        type: 'object',
        title: 'Y Axis',
        properties: {{
            min: {{
            type: 'number',
            title: 'Minimum Value',
            }},
            max: {{
            type: 'number',
            title: 'Maximum Value',
            }},
            title: {{
            type: 'object',
            title: 'Axis Title',
            properties: {{
                text: {{
                type: 'string',
                title: 'Text',
                }},
            }},
            }},
            plotLines: {{
            type: 'array',
            title: 'Plot Lines',
            items: {{
                type: 'object',
                properties: {{
                color: {{
                    type: 'string',
                    format: 'color',
                    title: 'Line Color',
                }},
                width: {{
                    type: 'number',
                    title: 'Line Width',
                }},
                value: {{
                    type: 'number',
                    title: 'Line Value',
                }},
                zIndex: {{
                    type: 'number',
                    title: 'Z-Index',
                }},
                label: {{
                    type: 'object',
                    title: 'Line Label',
                    properties: {{
                    text: {{
                        type: 'string',
                        title: 'Label Text',
                    }},
                    align: {{
                        type: 'string',
                        title: 'Label Alignment',
                        enum: ['left', 'center', 'right'],
                    }},
                    style: {{
                        type: 'object',
                        title: 'Label Style',
                        properties: {{
                        color: {{
                            type: 'string',
                            format: 'color',
                            title: 'Label Color',
                        }},
                        }},
                    }},
                    }},
                }},
                dashStyle: {{
                    type: 'string',
                    title: 'Line Style',
                    enum: [
                    'solid',
                    'shortdash',
                    'shortdot',
                    'shortdashdot',
                    'shortdashdotdot',
                    'dot',
                    'dash',
                    'longdash',
                    'dashdot',
                    'longdashdot',
                    'longdashdotdot',
                    ],
                }},
                }},
            }},
            }},
        }},
        }},
        plotOptions: {{
        type: 'object',
        title: 'Plot Options',
        properties: {{
            column: {{
            type: 'object',
            title: 'Column Options',
            properties: {{
                stacking: {{
                type: ['string', 'null'],
                title: 'Stacking Type',
                enum: ['normal', 'percent', null],
                enumNames: ['Normal', 'Percentage', 'None'],
                }},
                dataLabels: {{
                type: 'object',
                title: 'Data Labels',
                properties: {{
                    enabled: {{
                    type: 'boolean',
                    title: 'Enable Data Labels',
                    }},
                    formatter: {{
                    type: 'string',
                    title: 'Label Formatter',
                    description: 'JavaScript function as string to format the data label',
                    }},
                    inside: {{
                    type: 'boolean',
                    title: 'Place Labels Inside',
                    }},
                    align: {{
                    type: 'string',
                    title: 'Label Alignment',
                    enum: ['left', 'center', 'right'],
                    }},
                    color: {{
                    type: 'string',
                    format: 'color',
                    title: 'Label Color',
                    }},
                    style: {{
                    type: 'object',
                    title: 'Label Style',
                    properties: {{
                        fontSize: {{
                        type: 'string',
                        title: 'Font Size',
                        }},
                    }},
                    }},
                }},
                }},
            }},
            }},
        }},
        }},
        series: {{
        type: 'array',
        title: 'Data Series', // if there is only a single data series, make sure to still place it within an array.
        items: {{
            type: 'object',
            properties: {{
            name: {{
                type: 'string',
                title: 'Series Name',
            }},
            data: {{
                type: 'string',
                title: 'Data Points', // select from the provided column options and the data will be populated programmatically. if breakout is needed, provide a SINGLE series column and it will be programmatically broken out into multiple series by the breakout column (breakout column is set on a different top level property)
                enum: {column_options_string},
            }},
            color: {{
                type: 'string', // if using a breakout option, do not select a color and instead place a 'null' here.
                format: 'color',
                title: 'Series Color',
            }},
            }},
        }},
        }},
        legend: {{
        type: 'object',
        title: 'Legend Options',
        properties: {{
            enabled: {{
            type: 'boolean',
            title: 'Show Legend',
            }},
            align: {{
            type: 'string',
            title: 'Horizontal Alignment',
            enum: ['left', 'center', 'right'],
            enumNames: ['Left', 'Center', 'Right'],
            }},
            verticalAlign: {{
            type: 'string',
            title: 'Vertical Alignment',
            enum: ['top', 'middle', 'bottom'],
            enumNames: ['Top', 'Middle', 'Bottom'],
            }},
            layout: {{
            type: 'string',
            title: 'Layout Direction',
            enum: ['horizontal', 'vertical'],
            enumNames: ['Horizontal', 'Vertical'],
            }},
        }},
        }},
        "breakout": {{ // use this to breakout the data in the series field by the selected breakout column. leave blank if no breakout is desired.
            type: 'string',
            title: 'Breakout',
            enum: {column_options_string},
        }}
    }},
    }},
}},
}}
        """
        
        piecharts_waterfall_schema = f"""
const highchartsSchema = {{
type: 'object',
title: 'Highcharts Configuration',
properties: {{
    options: {{
    type: 'object',
    title: 'Chart Configuration',
    properties: {{
        chart: {{
        type: 'object',
        title: 'Chart Options',
        properties: {{
            type: {{
            type: 'string',
            title: 'Chart Type',
            enum: ['pie', 'waterfall'],
            enumNames: [
                'Pie',
                'Waterfall',
            ],
            }},
            polar: {{
            type: 'boolean',
            title: 'Polar Chart',
            }},
            alignTicks: {{
            type: 'boolean',
            title: 'Align Ticks',
            }},
            height: {{
            type: 'number',
            title: 'Chart Height',
            description: 'Height of the chart in pixels',
            }},
        }},
        }},
        title: {{
        type: 'object',
        title: 'Title Options',
        properties: {{
            text: {{
            type: 'string',
            title: 'Chart Title',
            }},
            style: {{
            type: 'object',
            title: 'Title Style',
            properties: {{
                fontSize: {{
                type: 'string',
                title: 'Font Size',
                }},
                fontWeight: {{
                type: 'string',
                title: 'Font Weight',
                enum: ['normal', 'bold', 'lighter', 'bolder'],
                }},
            }},
            }},
        }},
        }},
        subtitle: {{
            type: 'object',
            title: 'Subtitle Options',
            properties: {{
                text: {{
                    type: 'string', // optionally, provide a subtitle to the chart.
                    title: 'Subtitle Text',
                }},
            }},
        }},
        xAxis: {{ // only include xAxis if the chart type is waterfall. Ignore this for pie charts.
            type: 'object',
            title: 'X Axis Options',
            properties: {{
                type: {{
                    type: 'string',
                    enum: ['category'],
                }}
            }},
        }},
        yAxis: {{ // only include yAxis if the chart type is waterfall. Ignore this for pie charts.
            type: 'object',
            title: 'Y Axis Options',
            properties: {{
                title: {{
                    type: 'object',
                    title: 'Y Axis Title',
                    properties: {{
                        text: {{
                            type: 'string',
                            title: 'Y Axis Title Text',
                        }}
                    }}
                }}
            }},
        plotOptions: {{
        type: 'object',
        title: 'Plot Options',
        properties: {{
            pie: {{ //only include pie if the chart type is pie. Ignore this for waterfall charts.
                type: 'object',
                properties: {{
                    allowPointSelect: {{
                        type: 'boolean',
                        title: 'Allow Point Selection',
                    }},
                    cursor: {{
                        type: 'string',
                        title: 'Cursor',
                        enum: ['pointer', 'default'],
                    }},
                    dataLabels: {{
                        type: 'object',
                        title: 'Data Labels',
                        properties: {{
                            enabled: {{
                                type: 'boolean',
                                title: 'Enable Data Labels',
                            }},
                            distance: {{
                                type: 'number',
                                title: 'Distance',
                            }},
                            style: {{
                                type: 'object',
                                title: 'Style',
                                properties: {{
                                    color: {{
                                        type: 'string',
                                        title: 'Color',
                                    }},
                                    fontWeight: {{
                                        type: 'string',
                                        title: 'Font Weight',
                                        enum: ['normal', 'bold', 'lighter', 'bolder'],
                                    }},
                                }},
                            }},
                        }},
                    }}
                }},
            }}
        }},
        }},
        series: {{
        type: 'array',
        title: 'Data Series', // if there is only a single data series, make sure to still place it within an array.
        items: {{
            type: 'object',
            properties: {{
                labelColumn: {{ // this is the column that will be used to label the pie slices / waterfall steps.
                    type: 'string',
                    title: 'Label Column',
                    enum: {column_options_string},
                }},
                valueColumn: {{ // this is the column that will be used to determine the size of the pie slices / waterfall steps.
                    type: 'string',
                    title: 'Value Column',
                    enum: {column_options_string},
                }},
                dataLabels: {{ // only include dataLabels if the chart type is waterfall. Ignore this for pie charts.
                    type: 'object',
                    title: 'Data Labels',
                    properties: {{
                        enabled: {{
                            type: 'boolean',
                        }}
                    }}
                }}
            }},
        }},
        }},
        legend: {{
        type: 'object',
        title: 'Legend Options',
        properties: {{
            enabled: {{
            type: 'boolean',
            title: 'Show Legend',
            }},
            align: {{
            type: 'string',
            title: 'Horizontal Alignment',
            enum: ['left', 'center', 'right'],
            enumNames: ['Left', 'Center', 'Right'],
            }},
            verticalAlign: {{
            type: 'string',
            title: 'Vertical Alignment',
            enum: ['top', 'middle', 'bottom'],
            enumNames: ['Top', 'Middle', 'Bottom'],
            }},
            layout: {{
            type: 'string',
            title: 'Layout Direction',
            enum: ['horizontal', 'vertical'],
            enumNames: ['Horizontal', 'Vertical'],
            }},
        }},
        }},
    }},
    }},
}},
}},
}}
        """
        
        user_query_payload = f"""
        The data has been generated based on the following user query:
        {user_query}
        """
        if question_is_follow_up:
            user_query_payload = f"""
            ** This is a follow-up question **
            The user is asking for a modification to the previous visualization.
            Here is the previous question:
            {previous_question}

            Here is the previous visualization:
            {previous_vis_json}

            You will be provided with the same dataframe and the same highcharts schema as before.
            Attempt to incorporate the modifications requested by the user into the visualization.

            Here is the new user query:
            {user_query}
            """
            
        dataframe_str = df.head(10).to_string(index=False)

        prompt = f"""
            You are an expert in highcharts. You will be given a dataframe and a highcharts schema.
            You will need to return a highcharts JSON object that is a valid highcharts object. Take the user query into account when generating the highcharts object.

            {user_query_payload}

            Here are the first 10 rows of the dataframe:
            {dataframe_str}

            Here is the highcharts schema:
            {highcharts_schema}

            Alternatively, you may return a pie chart / waterfall chartschema if it is more appropriate for the data:
            {piecharts_waterfall_schema}

            Don't add a formatter function.

            Ensure that your output is JSON parseable. Conform to the highcharts schema.
            For the data, use the columns from the provided options and they will be populated programmatically.
            Respond with nothing except the JSON object.
            """
        
        response = arc.llm.chat_completion(messages=[{"role": "user", "content": prompt}], model_override=model_override)
        print(response)

        result = response['body']['choices'][0]['message']['content'].replace("```json", "").replace("```", "")

        try:
            parsed_result = json.loads(result)
        except Exception as e:
            print("caught invalid json from data visualization service, attempting to fix: " + str(e))
            try:
                followup_prompt = f"""
        The JSON object you generated caused an error when attempting to parse it with python's json.loads function.
        Here is the error:
        {e}

        Here is the JSON object you generated:
        {result}

        Please fix the JSON object and return the fixed JSON object. Remember to conform to the highcharts schema.

        As a reminder, here is the highcharts schema:
        {highcharts_schema}

        Alternatively, you may return a pie chart / waterfall chart schema if it is more appropriate for the data:
        {piecharts_waterfall_schema}

        As a reminder, here are the first 10 rows of the dataframe:
        {dataframe_str}

        Don't add a formatter function.
        Respond with nothing except the JSON object. Make sure to fix it so that it can be parsed by python's json.loads function. Make sure it is valid JSON that matches the provided highcharts schema.
        """
                messages = [
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": result},
                    {"role": "user", "content": followup_prompt}
                ]
                response = arc.llm.chat_completion(messages=messages, model_override=model_override)
                result = response['body']['choices'][0]['message']['content'].replace("```json", "").replace("```", "")
                parsed_result = json.loads(result)
            except Exception as e:
                return VisResult(success=False, error="invalid json output: " + str(e))
        print("BREAKOUT: " + str(parsed_result.get("options", {}).get("breakout", None)))
        print("CHART TYPE: " + str(parsed_result.get("options", {}).get("chart", {}).get("type", "")))
        print("Pre Hydration: " + json.dumps(parsed_result, indent=4))
        pie = parsed_result.get("options", {}).get("chart", {}).get("type", "") == "pie" or parsed_result.get("options", {}).get("chart", {}).get("type", "") == "waterfall"
        hydrated_json = hydrate_highcharts_json(parsed_result, df, breakout=parsed_result.get("options", {}).get("breakout", None), pie=pie)

        return VisResult(success=True, visualization=hydrated_json)

    except Exception as e:
        print("unexpected error in Data Visualization Service")
        print(str(e))
        return VisResult(success=False, error="unexpected error occurred: " + str(e))

def update_skill_memory_payload(skill_memory_payload: dict, arc: AnswerRocketClient):
    try:
        success = arc.chat.set_skill_memory_payload(skill_memory_payload)
        print("Skill memory update was " + ("successful" if success else "unsuccessful"))
    except Exception as e:
        print("Error setting skill memory payload: " + str(e))


if __name__ == '__main__':
    mock_input = DataExplorer.create_input(arguments={'user_chat_question_with_context': "show me sales over time for the top 5 segments in 2022", "question_is_follow_up": False})
    output = DataExplorer(mock_input)
    preview_skill(DataExplorer, output)
    print(output)