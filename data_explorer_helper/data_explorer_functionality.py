from dataclasses import dataclass
import json
import os
from answer_rocket import AnswerRocketClient, MetaDataFrame
import pandas as pd
from skill_framework import ExitFromSkillException, ExportData, SkillVisualization, SkillInput, SkillOutput
import numpy as np
import jinja2
from skill_framework.layouts import wire_layout
from ar_analytics.helpers.utils import get_dataset_id

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
class DataExplorerState:
    question: str | None = None
    formatted_df: pd.DataFrame | None = None
    unformatted_df: pd.DataFrame | None = None
    sql: str | None = None
    explanation: str | None = None
    error: str | None = None
    sql_row_limit_exceeded: bool | None = False
    base_df: pd.DataFrame | None = None
    title: str | None = "Data Explorer"
    column_metadata_map: dict | None = None
    visualization: dict | None = None
    base_df_id: str | None = None
@dataclass
class VisResult:
    success: bool
    visualization: dict | None = None
    error: str | None = None

def run_data_explorer(parameters: SkillInput) -> SkillOutput:
    """
    Data Explorer skill

    This skill is able to generate SQL queries from a natural language user query, return the retrieved data and the SQL query used to retrieve it, and attempt to generate a visualization from the retrieved data.

    """
    try:
        print("Starting DataExplorer")
        print("Parameters: " + str(parameters.arguments))
        success_but_empty = False

        user_query = parameters.arguments.user_chat_question_with_context
        data_explore_state = DataExplorerState(
            question=user_query
        )

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



        arc = AnswerRocketClient()
        print("SDK Connected OK" if arc.can_connect() else "SDK Connection Failed")


        print("Generating SQL...")
        arc.skill.update_loading_message("Generating SQL...")

        dataset_id = None

        try:
            dataset_id = get_dataset_id()
        except:
            pass

        copilot = arc.config.get_copilot()

        if dataset_id is None:
            dataset_id = copilot.dataset_id

        database_id = copilot.database_id

        sql_res = arc.data.run_sql_ai(
            dataset_id=dataset_id,
            database_id=database_id,
            question=user_query
        )
        if sql_res is None:
            print("SQLGenAi Service failed to return a response")
            data_explore_state.error = "SQLGenAi Service failed to return a response"
            raise ExitFromSkillException(message=data_explore_state.error, prompt_message="Let the user know that an error occurred, suggest that the user should try another question")
        if sql_res.success:
            print("SQLGenAi Service returned a response")
            data = sql_res.data
            columns = [col['name'] for col in data.get('columns', [])]
            rows = [row.get('data', []) for row in data.get('rows', [])]
            df = pd.DataFrame(rows, columns=columns)
            if df.shape[0] == 10000:
                data_explore_state.sql_row_limit_exceeded = True
            df = df.fillna(0)
            data_explore_state.base_df = df
            if df.empty:
                success_but_empty = True
        if sql_res.success and not success_but_empty:
            print("sql_res.sql: " + str(sql_res.sql))
            print("sql_res.explanation: " + str(sql_res.explanation))
            print("sql_res.title: " + str(sql_res.title))
            print("sql_res.column_metadata_map: " + str(sql_res.column_metadata_map))
            data_explore_state.sql = sql_res.sql
            data_explore_state.explanation = sql_res.explanation
            data_explore_state.title = sql_res.title
            print("saving column metadata map: " + str(sql_res.column_metadata_map))
            print("type of column metadata map: " + str(type(sql_res.column_metadata_map)))
            data_explore_state.column_metadata_map = sql_res.column_metadata_map
            # rename columns based on column metadata map
            for col in data_explore_state.base_df.columns:
                if col in data_explore_state.column_metadata_map:
                    data_explore_state.base_df = data_explore_state.base_df.rename(columns={col: data_explore_state.column_metadata_map[col].get("display_name", col)})
                    if data_explore_state.column_metadata_map[col].get("display_name", col) != col:
                        data_explore_state.column_metadata_map[data_explore_state.column_metadata_map[col].get("display_name", col)] = data_explore_state.column_metadata_map[col]
                        del data_explore_state.column_metadata_map[col]
            unformatted_df = data_explore_state.base_df.copy()
            unformatted_df = unformatted_df.applymap(lambda x: round(x, 2) if isinstance(x, (int, float)) and x > 1 else x)


            formatted_df = data_explore_state.base_df.copy()
            # apply formatting rules from sql_res.column_metadata_map[col].format_string (an example value for this is "%.2f")
            for col in data_explore_state.base_df.columns:
                if col in data_explore_state.column_metadata_map:
                    format_string = data_explore_state.column_metadata_map[col].get("format_string", None)
                    if format_string:
                        try:
                            formatted_df[col] = formatted_df[col].apply(
                                lambda x: format(x, format_string) if isinstance(x, (int, float)) else x
                            )
                        except Exception as e:
                            print(f"Error formatting column '{col}' with format string '{format_string}': {e}")

            if formatted_df.shape[0] > 100:
                formatted_df = formatted_df.head(100)
                df_truncated = True
                data_explore_layout_variables.update({
                    "truncate_message_hidden": False
                })
            else:
                df_truncated = False
            df_string = formatted_df.to_string(index=False)

            data_table_columns = [{"name": col} for col in formatted_df.columns]
            data_table_data = formatted_df.to_numpy().tolist()


            base_df_id = data_explore_state.base_df.max_metadata.get_id()
            base_df_description = f"A dataframe retrieved based on the user question: {data_explore_state.question}. Consists of the following columns: "
            column_descriptions = []
            for col in data_explore_state.column_metadata_map:
                col_info = data_explore_state.column_metadata_map[col]
                col_type = col_info.get("type", "")
                col_desc = col_info.get("description", "")
                column_descriptions.append(f"{col} ({col_type}): {col_desc}")

            base_df_description += ", ".join(column_descriptions)
            data_explore_state.base_df.max_metadata.set_description(base_df_description)
            data_table_desc = "A datatable displaying dataframe data. The data has been formatted to be more readable. " + ("The data has been truncated to 100 rows for display purposes." if df_truncated else "") + "The dataframe has the following associated description: \n" + base_df_description
            data_explore_layout_variables.update({
                "base_df_id": base_df_id,
                "data_table_element_description": data_table_desc,
            })
        else:
            if sql_res.sql is not None:
                print("SQLGenAi Service returned an error (or df is empty), but sql was generated")
                data_explore_state.error = sql_res.error

                data_explore_state.sql = sql_res.sql
                data_explore_state.explanation = sql_res.explanation
                data_explore_state.title = sql_res.title

                data_explore_layout_variables.update({
                    "error_hidden": True if success_but_empty else False,
                    "error_message": data_explore_state.error,
                    "sql_hidden": False,
                    "sql_text": "```sql\n" + format_sql(data_explore_state.sql),
                    "sql_explanation": data_explore_state.explanation,
                    "truncate_message_hidden": False if success_but_empty else True,
                    "truncate_message_text": "The query ran successfully, but no data was returned" if success_but_empty else "The SQL query reached the 10,000 row default limit. The data has also been truncated to 100 rows for display purposes." if data_explore_state.sql_row_limit_exceeded else None
                })
                rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
                # sql was generated but there was an error, surface error in output block and display sql
                #format output block
                if success_but_empty:
                    final_prompt = parameters.arguments.sql_success_empty_data_final_prompt
                else:
                    final_prompt = jinja2.Template(parameters.arguments.sql_error_final_prompt_template).render(error_message=data_explore_state.error)

                skill_output = SkillOutput(
                    final_prompt=final_prompt,
                    narrative="",
                    visualizations=[SkillVisualization(
                        title=data_explore_state.title,
                        layout = rendered_data_explore_layout
                    )]
                )
                return skill_output
            else:
                print("SQLGenAi Service returned an error, and no sql was generated")
                data_explore_state.error = sql_res.error
                raise ExitFromSkillException(message=data_explore_state.error, prompt_message="Let the user know that an error occurred, suggest that the user should try another question")

        arc.skill.update_loading_message("Generating visualization...")
        print("Generating visualization...")
        # Format the data in the structure expected by the visualization service
        columns = [{"name": col} for col in unformatted_df.columns]
        rows = [{"data": row} for row in unformatted_df.values.tolist()]

        vis_data_input = {
            "columns": columns,
            "rows": rows
        }
        print("sending column metadata map: " + str(data_explore_state.column_metadata_map))
        print("type of column metadata map: " + str(type(data_explore_state.column_metadata_map)))
        vis_result = arc.data.generate_visualization(
            data=vis_data_input,
            column_metadata_map=data_explore_state.column_metadata_map
        )
        if vis_result is None:
            print("Visualization service failed to return a response")
            data_explore_state.error = "Visualization service failed to return a response"
            data_explore_layout_variables.update({
                "error_hidden": True,
                "data_table_hidden": False,
                "data_table_columns": data_table_columns,
                "data_table_data": data_table_data,
                "sql_hidden": False,
                "sql_text": "```sql\n" + format_sql(data_explore_state.sql),
                "sql_explanation": data_explore_state.explanation
            })
            vis_type = None
            rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
            skill_output = SkillOutput(
                final_prompt=jinja2.Template(parameters.arguments.final_prompt_template).render(sql=data_explore_state.sql, df_string=df_string, df_truncated=df_truncated, vis_type=vis_type),
                narrative="",
                visualizations=[SkillVisualization(
                    title=data_explore_state.title,
                    layout = rendered_data_explore_layout
                )],
                export_data=[ExportData(
                    name="data_explorer_data",
                    data=data_explore_state.base_df
                )]
            )
            return skill_output
        if vis_result.success:
            data_explore_state.visualization = vis_result.visualization
            visualization_desc = f"A highcharts visualization of type {vis_result.visualization.get('options', {}).get('chart', {}).get('type', 'unknown')} of the dataframe data. The data has been formatted to be more readable. The underlying dataframe has the following associated description: \n" + base_df_description
            data_explore_layout_variables.update({
                "highcharts_element_description": visualization_desc
            })
        else:
            print("vis_result.error: " + str(vis_result.error))
            data_explore_state.error = vis_result.error
            data_explore_layout_variables.update({
                "error_hidden": True,
                "data_table_hidden": False,
                "data_table_columns": data_table_columns,
                "data_table_data": data_table_data,
                "sql_hidden": False,
                "sql_text": "```sql\n" + format_sql(data_explore_state.sql),
                "sql_explanation": data_explore_state.explanation
            })
            vis_type = None
            rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
            skill_output = SkillOutput(
                final_prompt=jinja2.Template(parameters.arguments.final_prompt_template).render(sql=data_explore_state.sql, df_string=df_string, df_truncated=df_truncated, vis_type=vis_type),
                narrative="",
                visualizations=[SkillVisualization(
                    title=data_explore_state.title,
                    layout = rendered_data_explore_layout
                )],
                export_data=[ExportData(
                    name="data_explorer_data",
                    data=data_explore_state.base_df
                )]
            )
            return skill_output



        data_explore_layout_variables.update({
            "visualization_hidden": False,
            "visualization": vis_result.visualization["options"],
            "data_table_hidden": False,
            "data_table_columns": data_table_columns,
            "data_table_data": data_table_data,
            "sql_hidden": False,
            "sql_text": "```sql\n" + format_sql(data_explore_state.sql),
            "sql_explanation": data_explore_state.explanation
        })
        rendered_data_explore_layout = wire_layout(data_explore_layout, data_explore_layout_variables)
        print("FINAL RENDERED ARTIFACT PANEL")
        print(rendered_data_explore_layout)
        print("(*)"*50)




        vis_type = data_explore_state.visualization.get("options", {}).get("chart", {}).get("type", None)

        skill_output = SkillOutput(
            final_prompt=jinja2.Template(parameters.arguments.final_prompt_template).render(sql=data_explore_state.sql, df_string=df_string, df_truncated=df_truncated, vis_type=vis_type),
            narrative="",
            visualizations=[SkillVisualization(
                title=data_explore_state.title,
                layout = rendered_data_explore_layout
            )],
            export_data=[ExportData(
                name="data_explorer_data",
                data=data_explore_state.base_df
            )]
        )
        return skill_output

    except Exception as e:
        if isinstance(e, ExitFromSkillException):
            raise e
        # unexpected error case
        print("Unexpected error encountered in DataExplorer: " + str(e))
        raise ExitFromSkillException(message=str(e), prompt_message="Let the user know that an unexpected error occurred, suggest that the user should try another question")
