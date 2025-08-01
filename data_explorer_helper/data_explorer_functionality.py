import json
from dataclasses import dataclass
from uuid import UUID

import jinja2
import logging
import pandas as pd
import sqlparse
from answer_rocket import AnswerRocketClient
from ar_analytics.helpers.utils import get_dataset_id
from skill_framework import ExitFromSkillException, ExportData, SkillVisualization, SkillInput, SkillOutput
from skill_framework.layouts import wire_layout

_logger = logging.getLogger("data_explorer")

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
    Generates SQL queries from natural language input, returns results, and creates visualizations.

    This function processes a natural language user query to:
    - Generate an appropriate SQL query.
    - Retrieve the data using that SQL query.
    - Return both the retrieved data and the SQL query.
    - Attempt to generate a visualization based on the retrieved data.

    Parameters
    ----------
    parameters : SkillInput
        Input object containing the natural language query and any required context.

    Returns
    -------
    SkillOutput
        Output object containing the SQL query, the data results, and any generated visualization.
    """
    try:
        _logger.info("Starting Data Explorer")
        _logger.info("Parameters: " + str(parameters.arguments))

        success_but_empty = False

        user_query = parameters.arguments.user_chat_question_with_context

        data_explore_state = DataExplorerState(question=user_query)

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
        _logger.info("SDK Connected OK" if arc.can_connect() else "SDK Connection Failed")

        _logger.info("Generating SQL...")
        arc.skill.update_loading_message("Generating SQL...")

        dataset_id: UUID | None = None

        try:
            dataset_id = get_dataset_id()
        except:
            _logger.warning("get_dataset_id() could not get dataset_id")

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
            data_explore_state.error = "SQLGenAi Service failed to return a response"

            _logger.info(data_explore_state.error)

            raise ExitFromSkillException(
                message=data_explore_state.error,
                prompt_message="Let the user know that an error occurred, suggest that the user should try another question"
            )

        if sql_res.success:
            _logger.info("SQLGenAi Service returned a response")

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

        if sql_res.timing_info:
            pretty_str = json.dumps(sql_res.timing_info, indent=4)
            _logger.info(f"Timing info:\n{pretty_str}")

        if sql_res.success and not success_but_empty:
            _dump_sql_ai_result(sql_res)

            data_explore_state.sql = sql_res.sql
            data_explore_state.explanation = sql_res.explanation
            data_explore_state.title = sql_res.title

            _logger.info("saving column metadata map: " + str(sql_res.column_metadata_map))
            _logger.info("type of column metadata map: " + str(type(sql_res.column_metadata_map)))

            data_explore_state.column_metadata_map = sql_res.column_metadata_map

            column_metadata_map = data_explore_state.column_metadata_map

            # rename columns based on column metadata map
            for col in data_explore_state.base_df.columns:
                if col in column_metadata_map:
                    data_explore_state.base_df = data_explore_state.base_df.rename(
                        columns={col: column_metadata_map[col].get("display_name", col)}
                    )

                    if column_metadata_map[col].get("display_name", col) != col:
                        column_metadata_map[column_metadata_map[col].get("display_name", col)] = column_metadata_map[col]

                        del column_metadata_map[col]

            unformatted_df = data_explore_state.base_df.copy()
            unformatted_df = unformatted_df.applymap(lambda x: round(x, 2) if isinstance(x, (int, float)) and x > 1 else x)

            formatted_df = data_explore_state.base_df.copy()

            from ar_analytics.helpers.utils import SharedFn

            helper = SharedFn()

            # apply formatting rules from sql_res.column_metadata_map[col].format_string (an example value for this is "%.2f")
            for col in data_explore_state.base_df.columns:
                if col in column_metadata_map:
                    format_string = column_metadata_map[col].get("format_string", None)

                    if format_string:
                        try:
                            formatted_df[col] = formatted_df[col].apply(
                                lambda x: helper.get_formatted_num(x, format_string) if isinstance(x, (int, float)) else x
                            )
                        except Exception as e:
                            _logger.info(f"Error formatting column '{col}' with format string '{format_string}': {e}")

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
                "data_table_element_description": data_table_desc
            })
        else:
            if sql_res.sql is not None:
                _logger.info("SQLGenAi Service returned an error (or df is empty), but sql was generated")

                _dump_sql_ai_result(sql_res)

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
                _logger.info("SQLGenAi Service returned an error, and no sql was generated")

                _dump_sql_ai_result(sql_res)

                data_explore_state.error = sql_res.error

                raise ExitFromSkillException(message=data_explore_state.error, prompt_message="Let the user know that an error occurred, suggest that the user should try another question")

        viz_msg = "Generating visualization..."
        _logger.info(viz_msg)
        arc.skill.update_loading_message(viz_msg)

        # Format the data in the structure expected by the visualization service
        columns = [{"name": col} for col in unformatted_df.columns]
        rows = [{"data": row} for row in unformatted_df.values.tolist()]

        vis_data_input = {
            "columns": columns,
            "rows": rows
        }

        _logger.info("sending column metadata map: " + str(data_explore_state.column_metadata_map))
        _logger.info("type of column metadata map: " + str(type(data_explore_state.column_metadata_map)))

        vis_result = arc.data.generate_visualization(
            data=vis_data_input,
            column_metadata_map=data_explore_state.column_metadata_map
        )

        if vis_result is None:
            data_explore_state.error = "Visualization service failed to return a response"

            _logger.info(data_explore_state.error)

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
            _logger.info("vis_result.error: " + str(vis_result.error))

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

        _logger.info("FINAL RENDERED ARTIFACT PANEL")
        _logger.info(rendered_data_explore_layout)
        _logger.info("(*)"*50)

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
        _logger.info("Unexpected error encountered in DataExplorer: " + str(e))

        raise ExitFromSkillException(message=str(e), prompt_message="Let the user know that an unexpected error occurred, suggest that the user should try another question")


def _dump_sql_ai_result(run_sql_ai_result):
    def pretty_json(label: str, data) -> str:
        """
        Pretty-print JSON data to logs. Handles strings, dicts, lists, or any input.

        Args:
            label (str): Label for context in logs (e.g., "column_metadata_map")
            data (Any): JSON string, dict, list, or any object to log
        """
        try:
            if isinstance(data, str):
                # Try to parse string as JSON
                parsed = json.loads(data)
            else:
                parsed = data  # Assume already dict/list/etc.

            pretty_str = json.dumps(parsed, indent=4)
        except:
            # Fallback to plain string if parsing or dumping fails
            pretty_str = str(data)

        return f"{label}\n{pretty_str.strip()}\n"

    def log_it(label: str, data):
        _logger.info(pretty_json(label, data))

    log_it("sql", run_sql_ai_result.sql)
    log_it("raw_sql", run_sql_ai_result.raw_sql)

    if run_sql_ai_result.rendered_prompt:
        try:
            messages = json.loads(run_sql_ai_result.rendered_prompt)

            log_messages = []

            for message in messages:
                log_message = f"""
role: {message["role"]}
content: {pretty_json("", message["content"])}
                """

                log_messages.append(log_message)

            _logger.info("rendered_prompt:\n" + "\n".join(log_messages))
        except:
            log_it("rendered_prompt", run_sql_ai_result.rendered_prompt)

    log_it("column_metadata_map", run_sql_ai_result.column_metadata_map)
    log_it("title", run_sql_ai_result.title)
    log_it("explanation", run_sql_ai_result.explanation)
    log_it("timing_info", run_sql_ai_result.timing_info)

    if hasattr(run_sql_ai_result, 'prior_runs'):
        for prior_run in run_sql_ai_result.prior_runs:
            _logger.info("====================== Prior Run: ======================")
            _dump_sql_ai_result(prior_run)
