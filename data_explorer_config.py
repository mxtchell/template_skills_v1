FINAL_PROMPT_TEMPLATE = """All of the following information has already been provided to the USER. You should not restate the data but rather only surface any key observations. If there are no key observations it is acceptable to simply state "The information has been provided in the artifacts panel."

        The following SQL query was executed:
        {{sql}}

        The following data was returned {% if df_truncated %} (truncated to 100 rows for display purposes, keep in mind that you are only seeing a sample of this data if asked questions about the data) {% endif %}:
        {{df_string}}

        {% if vis_type %}
            A visualization of type {{vis_type}} was generated from the data. (don't display this to the user, but if they wish to modify the visualization, you will need to run this skill again for them)
        {% endif %}"""
SQL_SUCCESS_EMPTY_DATA_FINAL_PROMPT = "The SQL query was generated, but the data returned was empty. Let the user know that the SQL query was generated, but the data returned was empty. Suggest that the user should try another question"

SQL_ERROR_FINAL_PROMPT_TEMPLATE = "The following error occurred while generating the SQL query: {{error_message}}. Let the user know that an error occurred, suggest that the user should try another question"


DATA_EXPLORE_LAYOUT = r"""{
	"layoutJson": {
		"type": "Document",
		"rows": 100,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "max-content",
			"padding": "15px",
			"gap": "10px"
		},
		"children": [
			{
				"name": "CardContainer2-3",
				"type": "CardContainer",
				"children": "",
				"minHeight": "40px",
				"rows": 1,
				"columns": 1,
				"maxHeight": "40px",
				"style": {
					"borderRadius": "6.197px",
					"backgroundColor": "#FEF2F2",
					"padding": "10px",
					"paddingLeft": "20px",
					"paddingRight": "20px"
				},
				"hidden": false,
				"row": 1,
				"column": 1
			},
			{
				"name": "FlexContainer2",
				"type": "FlexContainer",
				"children": "",
				"minHeight": "0px",
				"direction": "column",
				"hidden": false
			},
			{
				"name": "CardContainer2",
				"type": "CardContainer",
				"children": "",
				"minHeight": "40px",
				"rows": 1,
				"columns": 1,
				"maxHeight": "40px",
				"style": {
					"borderRadius": "6.197px",
					"background": "#EFF6FF",
					"padding": "10px",
					"paddingLeft": "20px",
					"paddingRight": "20px"
				},
				"hidden": false
			},
			{
				"name": "FlexContainer0-3",
				"type": "FlexContainer",
				"children": "",
				"direction": "column",
				"minHeight": "",
				"maxHeight": "",
				"style": {
					"borderRadius": "11.911px",
					"background": "var(--White, #FFF)",
					"box-shadow": "0px 0px 8.785px 0px rgba(0, 0, 0, 0.10) inset",
					"padding": "10px",
					"fontFamily": "Arial"
				},
				"flexDirection": "row",
				"hidden": false,
				"row": 1,
				"column": 1
			},
			{
				"type": "HighchartsChart",
				"width": null,
				"height": null,
				"children": "",
				"name": "Highcharts Chart",
				"options": {
					"chart": {
						"type": "pie",
						"polar": false,
						"alignTicks": true,
						"height": 400
					},
					"title": {
						"text": "Top 8 Segments by Total Sales (2022)",
						"style": {
							"fontSize": "18px",
							"fontWeight": "bold"
						}
					},
					"plotOptions": {
						"pie": {
							"allowPointSelect": true,
							"cursor": "pointer",
							"dataLabels": {
								"enabled": true,
								"distance": 30,
								"style": {
									"color": "#000000",
									"fontWeight": "bold"
								}
							}
						}
					},
					"series": [
						{
							"labelColumn": "segment",
							"valueColumn": "total_sales",
							"data": [
								{
									"name": "sample",
									"y": 799834847.8699951
								},
								{
									"name": "chart",
									"y": 798769147.5819905
								},
								{
									"name": "FILLED PASTA",
									"y": 568545246.4949996
								},
								{
									"name": "BAKING",
									"y": 117811767.06799985
								},
								{
									"name": "this",
									"y": 39237618.809
								},
								{
									"name": "is",
									"y": 20666835.681
								},
								{
									"name": "a",
									"y": 1366.1100000000001
								}
							],
							"colorByPoint": true,
							"name": "total_sales"
						}
					],
					"legend": {
						"enabled": true,
						"align": "center",
						"verticalAlign": "bottom",
						"layout": "horizontal"
					}
				},
				"row": null,
				"column": null,
				"parentId": "FlexContainer0-3"
			},
			{
				"name": "tableBlock",
				"type": "DataTable",
				"width": null,
				"height": null,
				"columns": [
					{
						"name": "column 1"
					},
					{
						"name": "column 2"
					},
					{
						"name": "column 3"
					}
				],
				"data": [
					[
						"col1row1",
						"col2row1",
						"col3row1"
					],
					[
						"col1row2",
						"col2row2",
						"col3row3"
					]
				],
				"styles": {
					"alternateRowColor": "#f0fff0",
					"fontFamily": "Arial, sans-serif",
					"th": {
						"backgroundColor": "#FOFOFO",
						"color": "#000000",
						"fontWeight": "bold"
					},
					"caption": {
						"backgroundColor": "#32ea05",
						"color": "#000000",
						"fontWeight": "bold",
						"fontSize": "10pt"
					}
				},
				"row": null,
				"column": null,
				"parentId": "FlexContainer0-4"
			},
			{
				"name": "CardContainer0",
				"type": "CardContainer",
				"children": "",
				"minHeight": "0px",
				"rows": 2,
				"columns": 1
			},
			{
				"name": "Header0",
				"type": "Header",
				"children": "",
				"text": "SQL",
				"style": {
					"fontSize": "20px",
					"fontWeight": "700",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"borderBottom": "solid #DDD 2px"
				},
				"parentId": "FlexContainer0",
				"flex": "shrink",
				"hidden": false
			},
			{
				"name": "Markdown0",
				"type": "Markdown",
				"children": "",
				"text": "```sql\nSELECT\n    c.customer_id,\n    c.customer_name,\n    SUM(p.amount) AS total_spend\nFROM\n    customers c\nJOIN\n    purchases p ON c.customer_id = p.customer_id\nWHERE\n    p.purchase_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)\nGROUP BY\n    c.customer_id,\n    c.customer_name\nHAVING\n    total_spend > 1000\nORDER BY\n    total_spend DESC\nLIMIT 10;",
				"style": {
					"color": "#555",
					"backgroundColor": "#ffffff",
					"border": "none",
					"fontSize": "15px"
				},
				"parentId": "FlexContainer0",
				"flex": "grow",
				"hidden": false
			},
			{
				"name": "FlexContainer0-4",
				"type": "FlexContainer",
				"children": "",
				"direction": "column",
				"minHeight": "",
				"maxHeight": "",
				"style": {
					"borderRadius": "11.911px",
					"background": "var(--White, #FFF)",
					"box-shadow": "0px 0px 8.785px 0px rgba(0, 0, 0, 0.10) inset",
					"padding": "10px",
					"fontFamily": "Arial"
				},
				"flexDirection": "row",
				"hidden": false,
				"row": 1,
				"column": 1
			},
			{
				"name": "FlexContainer0",
				"type": "FlexContainer",
				"children": "",
				"direction": "column",
				"minHeight": "",
				"maxHeight": "",
				"style": {
					"borderRadius": "11.911px",
					"background": "var(--White, #FFF)",
					"box-shadow": "0px 0px 8.785px 0px rgba(0, 0, 0, 0.10) inset",
					"padding": "10px",
					"fontFamily": "Arial"
				},
				"flexDirection": "row",
				"hidden": false
			},
			{
				"name": "FlexContainer1",
				"type": "FlexContainer",
				"children": "",
				"direction": "column",
				"minHeight": "",
				"maxHeight": "",
				"style": {
					"borderRadius": "11.911px",
					"background": "var(--White, #FFF)",
					"box-shadow": "0px 0px 8.785px 0px rgba(0, 0, 0, 0.10) inset",
					"padding": "10px",
					"fontFamily": "Arial"
				},
				"flexDirection": "row",
				"hidden": false
			},
			{
				"name": "Header1",
				"type": "Header",
				"children": "",
				"text": "SQL Explanation",
				"style": {
					"fontSize": "20px",
					"fontWeight": "700",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"borderBottom": "solid #DDD 2px"
				},
				"parentId": "FlexContainer1",
				"flex": "shrink"
			},
			{
				"name": "Markdown1",
				"type": "Markdown",
				"children": "",
				"text": "This query retrieves the top 10 customers who have spent more than **$1,000** in the past year.\n\n- It joins the `customers` and `purchases` tables on `customer_id`.\n- Filters purchases to only those within the last **12 months**:\n  - Uses `WHERE p.purchase_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)`\n- Groups results by `customer_id` and `customer_name` to calculate `total_spend`.\n- Uses a `HAVING` clause to exclude customers with less than `$1,000` in spend.\n- Sorts results in **descending** order of `total_spend`.\n- Limits output to the **top 10 customers** using `LIMIT 10`.",
				"style": {
					"fontSize": "16px",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none"
				},
				"parentId": "FlexContainer1",
				"flex": "grow"
			},
			{
				"name": "Header2",
				"type": "Header",
				"width": 1,
				"children": "",
				"text": "The data was truncated to 100 rows for display purposes",
				"style": {
					"fontSize": "14px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#1D4ED8",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "start",
					"fontFamily": ""
				},
				"parentId": "CardContainer2",
				"hidden": false
			},
			{
				"name": "Header2-4",
				"type": "Header",
				"children": "",
				"text": "Error message",
				"style": {
					"fontSize": "14px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#B91C1C",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "start",
					"fontFamily": ""
				},
				"parentId": "CardContainer2-3",
				"hidden": false
			}
		]
	},
	"inputVariables": [
		{
			"name": "data_table_columns",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "tableBlock",
					"fieldName": "columns"
				}
			]
		},
		{
			"name": "data_table_data",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "tableBlock",
					"fieldName": "data"
				}
			]
		},
		{
			"name": "data_table_hidden",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "FlexContainer0-4",
					"fieldName": "hidden"
				},
				{
					"elementName": "tableBlock",
					"fieldName": "hidden"
				}
			]
		},
		{
			"name": "sql_text",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "sqlBlock",
					"fieldName": "text"
				},
				{
					"elementName": "Markdown0",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "sql_explanation",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "explanationBlock",
					"fieldName": "text"
				},
				{
					"elementName": "Markdown1",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "sql_hidden",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "sqlBlock",
					"fieldName": "hidden"
				},
				{
					"elementName": "explanationBlock",
					"fieldName": "hidden"
				},
				{
					"elementName": "FlexContainer0",
					"fieldName": "hidden"
				},
				{
					"elementName": "FlexContainer1",
					"fieldName": "hidden"
				},
				{
					"elementName": "Header0",
					"fieldName": "hidden"
				},
				{
					"elementName": "Markdown0",
					"fieldName": "hidden"
				},
				{
					"elementName": "Header1",
					"fieldName": "hidden"
				},
				{
					"elementName": "Markdown1",
					"fieldName": "hidden"
				}
			]
		},
		{
			"name": "visualization",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Highcharts Chart",
					"fieldName": "options"
				}
			]
		},
		{
			"name": "error_message",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "errorBlock",
					"fieldName": "text"
				},
				{
					"elementName": "Header2-4",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "error_hidden",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "CardContainer2-3",
					"fieldName": "hidden"
				},
				{
					"elementName": "Header2-4",
					"fieldName": "hidden"
				}
			]
		},
		{
			"name": "visualization_hidden",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "FlexContainer0-3",
					"fieldName": "hidden"
				}
			]
		},
		{
			"name": "truncate_message_hidden",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "truncateMessageBlock",
					"fieldName": "hidden"
				},
				{
					"elementName": "CardContainer2",
					"fieldName": "hidden"
				},
				{
					"elementName": "Header2",
					"fieldName": "hidden"
				}
			]
		},
		{
			"name": "truncate_message_text",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "truncateMessageBlock",
					"fieldName": "text"
				},
				{
					"elementName": "Header2",
					"fieldName": "text"
				}
			]
		}
	]
}"""