from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Dict, List

import pandas as pd
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, SuggestedQuestion, \
    ParameterDisplayDescription, ExportData
from skill_framework.preview import preview_skill
from skill_framework.layouts import wire_layout

from ar_analytics import ArUtils
from ar_analytics.helpers.utils import SharedFn

import jinja2
import logging
import uuid

from core_skill_code.config import LEVELS_FOR_DATA_PULL, OVER_UNDER_VARIANCE_THRESHOLD, \
    VARIANCE_METRIC_COL, VARIANCE_DIFF_COL, VARIANCE_DIFF_PER_COL, GROWTH_DIFF_COL, GROWTH_DIFF_PER_COL
from core_skill_code.expense_drivers_core import ExpenseDriversCoreTemplateParameterSetup, ExpenseDriversCore, \
    dim_tab_map, Tabs, ExpenseDriversHelpers

from ar_analytics.defaults import get_table_layout_vars

logger = logging.getLogger(__name__)


MAX_PROMPT = """
Answer user question in 30 words or less using following facts: {{facts}}
"""

INSIGHT_PROMPT = """
{{base_prompt}} Write a short headline followed by a 60 word or less paragraph about using facts below.
Use the structure from the 2 examples below to learn how I typically write summary.
Base your summary solely on the provided facts, avoiding assumptions or judgments.
Ensure clarity and accuracy.
Use markdown formatting for a structured and clear presentation.
{{facts}}
"""

SUMMARY_TEMPLATE = """

{
	"layoutJson": {
		"type": "Document",
		"rows": 90,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "max-content",
			"padding": "15px",
			"gap": "20px"
		},
		"children": [
			{
				"name": "CardContainer0",
				"type": "CardContainer",
				"children": "",
				"minHeight": "80px",
				"rows": 2,
				"columns": 1,
				"style": {
					"border-radius": "11.911px",
					"background": "#2563EB",
					"padding": "10px",
					"fontFamily": "Arial"
				},
				"hidden": false
			},
			{
				"name": "Header0",
				"type": "Header",
				"children": "",
				"text": "SMG&A | Salary - Expense Drivers",
				"style": {
					"fontSize": "20px",
					"fontWeight": "700",
					"color": "#ffffff",
					"textAlign": "left",
					"alignItems": "center"
				},
				"parentId": "CardContainer0",
				"hidden": false
			},
			{
				"name": "Paragraph0",
				"type": "Paragraph",
				"children": "",
				"text": "Actual vs. AC | Jan 2023-Mar 2023 | YTG",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#fafafa",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "center"
				},
				"parentId": "CardContainer0",
				"hidden": false
			},
			{
				"name": "CardContainer1",
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
				"parentId": "FlexContainer3",
				"row": null,
				"column": null,
				"height": null,
				"width": null
			},
			{
				"name": "Header1",
				"type": "Header",
				"children": "",
				"text": "Key Highlights",
				"style": {
					"fontSize": "15px",
					"fontWeight": "600",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#1E345C",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"borderBottom": "solid #DDD 2px",
					"padding": 10
				},
				"parentId": "CardContainer1",
				"flex": "",
				"hidden": false
			},
			{
				"name": "FlexContainer2",
				"type": "FlexContainer",
				"children": "",
				"minHeight": "250px",
				"direction": "column",
				"row": null,
				"column": null,
				"height": null,
				"width": null
			},
			{
				"name": "FlexContainer3",
				"type": "FlexContainer",
				"children": "",
				"minHeight": "450px",
				"direction": "row",
				"style": {
					"border": "solid #DDD 1.5px",
					"padding": "10px",
					"borderRadius": "11.911px"
				},
				"maxHeight": "1500px"
			},
			{
				"name": "CombinedPerformanceCard",
				"type": "Markdown",
				"text": "The Selling, General & Administrative (SMG&A) costs for 2024 stood at $519M compared to an allocated cost of $520M, resulting in a negligible variance of 0.17%. Within SMG&A, key contributors included **People costs** at $286M (constituting 55.17% of actual costs), **Non-People costs** at $233M (44.83% of actual costs), and **FTE** at 905. No notable favorability was observed in the overall SMG&A results.\n\nIn the Research and Development (R&D) segment, actual costs reached $71M, meeting a budgeted figure of $74M. This variance of 3.75% was driven exclusively by Non-People costs, which showed a favorable variance contribution of 100%.\n\nAcross the functions and cost types, key favorable variance contributors within SMG&A included **Compensation and Benefits**, showcasing favorable variances in categories such as **Salary** (13.71%), **Fixed Pension** (69.48%), and **Other Variable Benefits** (30.02%). Similarly, under **Information Systems**, drivers like **Application Maintenance **(9.29% variance) and **Enhancements & Upgrades**(27.13%) contributed significantly to the favorable trends.\n\nLastly, within business units, **AMEA HQ** exhibited noteworthy efficiency with a favorable variance of 3.61%, while Total India followed closely with a 4.82% favorable variance, led by strong performance in **India** operations. Conversely, performance in **MENAP **showed a mixed trend, where **UAE** experienced an unfavorable variance, offset by favorable performance in Saudi Arabia.\n\nThis analysis highlights moderate success in cost management for 2024, driven by targeted improvements in key areas such as employee compensation, IT operations, and strategic regional activities.",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"margin-left": "2em",
					"margin-right": "2em"
				},
				"parentId": "CardContainer1"
			},
			{
				"name": "DataTable0",
				"type": "DataTable",
				"children": "",
				"columns": [
					{
						"name": ""
					},
					{
						"name": "Actual"
					},
					{
						"name": "AC"
					},
					{
						"name": "Variance"
					},
					{
						"name": "Variance %"
					},
					{
						"name": "YTG"
					},
					{
						"name": "Outlook"
					}
				],
				"data": [
					[
						"SMGA | Salary",
						0,
						0,
						0
					],
					[
						"Row 2",
						10,
						10,
						10
					],
					[
						"Row 3",
						20,
						20,
						20
					],
					[
						"Row 4",
						30,
						30,
						30
					],
					[
						"Row 5",
						40,
						40,
						40
					],
					[
						"Row 6",
						50,
						50,
						50
					],
					[
						"Row 7",
						60,
						60,
						60
					]
				],
				"parentId": "FlexContainer2",
				"footer": "<span style='color: white;'>---</span><span style='color: white;'>---</span>**Currency Values are in 000s <span style='color: white;'>---</span>**sorted by highest contributors to variance",
				"caption": "Top Drivers of Variance",
				"styles": {
					"th": {
						"textAlign": "center"
					},
					"style": {
						"border": "solid #DDD 1.5px",
						"padding": "10px",
						"borderRadius": "11.911px"
					},
					"footer": {
						"textAlign": "left"
					},
					"alternateRowColor": "#ffffff"
				},
				"extraStyles": ""
			}
		]
	},
	"inputVariables": [
		{
			"name": "OverviewTitle",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header0",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "OverviewFilterSubTitle",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Paragraph0",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "OverviewInsights",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "CombinedPerformanceCard",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "table_data",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "DataTable0",
					"fieldName": "data"
				}
			]
		},
		{
			"name": "col_defs",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "DataTable0",
					"fieldName": "columns"
				}
			]
		}
	]
}
"""

DIM_TEMPLATE = """
{
	"layoutJson": {
		"type": "Document",
		"rows": 90,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "max-content",
			"padding": "15px",
			"gap": "20px"
		},
		"children": [
			{
				"name": "CardContainer0",
				"type": "CardContainer",
				"children": "",
				"minHeight": "80px",
				"rows": 2,
				"columns": 1,
				"style": {
					"border-radius": "11.911px",
					"background": "#2563EB",
					"padding": "10px",
					"fontFamily": "Arial"
				},
				"hidden": false
			},
			{
				"name": "Header0",
				"type": "Header",
				"children": "",
				"text": "Package & Sub Package Analysis",
				"style": {
					"fontSize": "20px",
					"fontWeight": "700",
					"color": "#ffffff",
					"textAlign": "left",
					"alignItems": "center"
				},
				"parentId": "CardContainer0",
				"hidden": false
			},
			{
				"name": "Paragraph0",
				"type": "Paragraph",
				"children": "",
				"text": "Actual vs. AC | Jan 2023-Mar 2023 | YTG",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#fafafa",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "center"
				},
				"parentId": "CardContainer0",
				"hidden": false
			},
			{
				"name": "CardContainer1",
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
				"parentId": "FlexContainer1",
				"row": null,
				"column": null,
				"height": null,
				"width": null
			},
			{
				"name": "Header1",
				"type": "Header",
				"children": "",
				"text": "Package and Sub Packages Highlights",
				"style": {
					"fontSize": "15px",
					"fontWeight": "600",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#1E345C",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"borderBottom": "solid #DDD 2px",
					"padding": 10
				},
				"parentId": "CardContainer1",
				"flex": "",
				"hidden": false
			},
			{
				"name": "FlexContainer3",
				"type": "FlexContainer",
				"children": "",
				"minHeight": "450px",
				"direction": "row",
				"style": {
					"border": "solid #DDD 1.5px",
					"padding": "10px",
					"borderRadius": "11.911px"
				},
				"maxHeight": "1500px",
				"hidden": false
			},
			{
				"name": "FlexContainer2",
				"type": "FlexContainer",
				"children": "",
				"minHeight": "250px",
				"direction": "column",
				"row": null,
				"column": null,
				"height": null,
				"width": null
			},
			{
				"name": "FlexContainer1",
				"type": "FlexContainer",
				"children": "",
				"minHeight": "250px",
				"direction": "column",
				"maxHeight": "",
				"style": {
					"gap": "20px"
				},
				"hidden": false
			},
			{
				"name": "CombinedPerformanceCard",
				"type": "Markdown",
				"text": "SMG&A for June YTD 2024 is favorable/unfavorable by **XX% / (X.XX%)** or **$XXXK / ($XXXK)**\n**Packages (Non-People Cost)** – Top contributors are Package 1 **$XXX / ($XXX)**, Package 2 **$XXX / ($XXX)**, Package 3 **$XXX / ($XXX)**, Package 4 **$XXX / ($XXX)**, and Package 5 **$XXX / ($XXX)**, representing **XX% / (X.XX%)**, **XX% / (X.XX%)**, **XX% / (X.XX%)**, **XX% / (X.XX%)**, and **XX% / (X.XX%)** of favorable/unfavorable variance respectively.",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"margin-left": "2em",
					"margin-right": "2em"
				},
				"parentId": "CardContainer1"
			},
			{
				"name": "HighchartsChart0",
				"type": "HighchartsChart",
				"children": "",
				"minHeight": "400px",
				"chartOptions": {
					"chart": {
						"type": "bar"
					},
					"title": {
						"text": "Sample Highchart"
					},
					"xAxis": {
						"categories": [
							"Category A",
							"Category B",
							"Category C"
						],
						"labels": {
							"enabled": true
						},
						"gridLineWidth": 0
					},
					"yAxis": {
						"title": {
							"text": "Values"
						},
						"labels": {
							"enabled": false
						},
						"gridLineWidth": 0
					},
					"series": [
						{
							"name": "Series 1",
							"data": [
								10,
								20,
								30
							],
							"color": "#94A3B8",
							"dataLabels": {
								"enabled": true
							}
						}
					],
					"plotOptions": {
						"bar": {
							"dataLabels": {
								"enabled": true
							}
						}
					}
				},
				"options": {
					"chart": {
						"type": "bar",
						"polar": false,
						"alignTicks": false
					},
					"title": {
						"text": "SMG&A Top Variance",
						"style": {
							"fontSize": "15px"
						}
					},
					"xAxis": {
						"categories": [
							"Category A",
							"Category B",
							"Category C",
							"Category D",
							"Category E",
							"Category F",
							"Category G",
							"Category H"
						],
						"title": {
							"text": ""
						},
						"labels": {
							"enabled": true
						},
						"gridLineWidth": 0
					},
					"yAxis": {
						"title": {
							"text": ""
						},
						"labels": {
							"enabled": false
						},
						"gridLineWidth": 0
					},
					"series": [
						{
							"name": "Actual",
							"data": [
								10.21312312,
								20,
								30,
								40,
								50,
								60,
								70,
								100
							],
							"dataLabels": {
								"enabled": true
							}
						},
						{
							"name": "Budget",
							"data": [
								15,
								25,
								35,
								105,
								33,
								45,
								88,
								44
							],
							"color": "#D1D5DB",
							"dataLabels": {
								"enabled": true
							}
						}
					],
					"credits": {
						"enabled": true
					},
					"legend": {
						"enabled": true,
						"align": "right",
						"verticalAlign": "middle",
						"layout": "vertical"
					},
					"plotOptions": {
						"bar": {
							"dataLabels": {
								"enabled": true,
								"format": "${point.y:,.0f}",
								"style": {
									"fontSize": "11px"
								}
							}
						},
						"column": {
							"dataLabels": {
								"style": {
									"fontSize": "11px"
								},
								"enabled": true,
								"inside": false
							},
							"stacking": "normal"
						}
					}
				},
				"parentId": "FlexContainer3",
				"flex": "1"
			},
			{
				"name": "DataTable0",
				"type": "DataTable",
				"children": "",
				"columns": [
					{
						"name": ""
					},
					{
						"name": "Actual"
					},
					{
						"name": "AC"
					},
					{
						"name": "Variance"
					},
					{
						"name": "Variance %"
					},
					{
						"name": "YTG"
					},
					{
						"name": "Outlook"
					}
				],
				"data": [
					[
						"Row 1",
						0,
						0,
						0
					],
					[
						"Row 2",
						10,
						10,
						10
					],
					[
						"Row 3",
						20,
						20,
						20
					],
					[
						"Row 4",
						30,
						30,
						30
					],
					[
						"Row 5",
						40,
						40,
						40
					],
					[
						"Row 6",
						50,
						50,
						50
					],
					[
						"Row 7",
						60,
						60,
						60
					]
				],
				"parentId": "FlexContainer2",
				"footer": "<span style='color: white;'>---</span><span style='color: white;'>---</span>**Currency Values are in 000s <span style='color: white;'>---</span>**Sorted by highest contributors to variance",
				"caption": "Top Drivers of Variance",
				"styles": {
					"th": {
						"textAlign": "center"
					},
					"style": {
						"border": "solid #DDD 1.5px",
						"padding": "10px",
						"borderRadius": "11.911px"
					},
					"footer": {
						"textAlign": "left"
					},
					"alternateRowColor": "#ffffff"
				},
				"extraStyles": ""
			},
			{
				"name": "HighchartsChart1",
				"type": "HighchartsChart",
				"children": "",
				"minHeight": "400px",
				"chartOptions": {
					"chart": {
						"type": "bar"
					},
					"title": {
						"text": "Sample Highchart"
					},
					"xAxis": {
						"categories": [
							"Category A",
							"Category B",
							"Category C"
						],
						"labels": {
							"enabled": true
						},
						"gridLineWidth": 0
					},
					"yAxis": {
						"title": {
							"text": "Values"
						},
						"labels": {
							"enabled": false
						},
						"gridLineWidth": 0
					},
					"series": [
						{
							"name": "Series 1",
							"data": [
								10,
								20,
								30
							],
							"dataLabels": {
								"enabled": true
							}
						}
					],
					"plotOptions": {
						"bar": {
							"dataLabels": {
								"enabled": true
							}
						}
					}
				},
				"options": {
					"chart": {
						"type": "bar",
						"polar": false
					},
					"title": {
						"text": "R&D Top Variance",
						"style": {
							"fontSize": "15px"
						}
					},
					"xAxis": {
						"categories": [
							"Category A",
							"Category B",
							"Category C"
						],
						"title": {
							"text": ""
						},
						"labels": {
							"enabled": true
						},
						"gridLineWidth": 0
					},
					"yAxis": {
						"title": {
							"text": ""
						},
						"labels": {
							"enabled": false
						},
						"gridLineWidth": 0
					},
					"series": [
						{
							"name": "Actual",
							"data": [
								10,
								20
							],
							"dataLabels": {
								"enabled": true
							}
						},
						{
							"name": "Budget",
							"data": [
								15,
								25
							],
							"color": "#D1D5DB",
							"dataLabels": {
								"enabled": true
							}
						}
					],
					"credits": {
						"enabled": true
					},
					"legend": {
						"enabled": true,
						"align": "right",
						"verticalAlign": "middle",
						"layout": "vertical"
					},
					"plotOptions": {
						"bar": {
							"dataLabels": {
								"enabled": true,
								"style": {
									"fontSize": "11px"
								}
							}
						},
						"column": {
							"dataLabels": {
								"style": {
									"fontSize": "11px"
								},
								"enabled": true
							}
						}
					}
				},
				"parentId": "FlexContainer3",
				"flex": "1",
				"hidden": false
			}
		]
	},
	"inputVariables": [
		{
			"name": "DimTitle",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header0",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "OverviewFilterSubTitle",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Paragraph0",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "OverviewInsights",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "CombinedPerformanceCard",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "OverviewTitle",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header1",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "col_defs",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "DataTable0",
					"fieldName": "columns"
				}
			]
		},
		{
			"name": "table_data",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "DataTable0",
					"fieldName": "data"
				}
			]
		},
		{
			"name": "hide_secondary_chart",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "hidden"
				}
			]
		},
		{
			"name": "primary_categories",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart0",
					"fieldName": "options.xAxis.categories"
				}
			]
		},
		{
			"name": "primary_data",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart0",
					"fieldName": "options.series"
				}
			]
		},
		{
			"name": "secondary_categories",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "options.xAxis.categories"
				}
			]
		},
		{
			"name": "secondary_data",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "options.series"
				}
			]
		},
		{
			"name": "primary_chart_title",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart0",
					"fieldName": "options.title.text"
				}
			]
		},
		{
			"name": "secondary_chart_title",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "options.title.text"
				}
			]
		},
		{
			"name": "primary_chart_data_label",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart0",
					"fieldName": "options.plotOptions.column.dataLabels"
				}
			]
		},
		{
			"name": "secondary_chart_data_label",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "options.plotOptions.column.dataLabels"
				}
			]
		},
		{
			"name": "secondary_yaxis",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "options.yAxis"
				}
			]
		},
		{
			"name": "primary_yaxis",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart0",
					"fieldName": "options.yAxis"
				}
			]
		},
		{
			"name": "primary_plot_options",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart0",
					"fieldName": "options.plotOptions"
				}
			]
		},
		{
			"name": "secondary_plot_options",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "HighchartsChart1",
					"fieldName": "options.plotOptions"
				}
			]
		}
	]
}
"""

@skill(
    name="Expense Drivers",
    llm_name="",
    description="",
    parameters=[
        SkillParameter(
            name="periods",
            constrained_to="date_filter",
            is_multi=True,
            description="If provided by the user, list time periods in a format 'q2 2023', '2021', 'jan 2023', 'mat nov 2022', 'mat q1 2021', 'ytd q4 2022', 'ytd 2023', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>'. Use knowledge about today's date to handle relative periods and open ended periods. If given a range, for example 'last 3 quarters, 'between q3 2022 to q4 2023' etc, enumerate the range into a list of valid dates. Don't include natural language words or phrases, only valid dates like 'q3 2023', '2022', 'mar 2020', 'ytd sep 2021', 'mat q4 2021', 'ytd q1 2022', 'ytd 2021', 'ytd', 'mat', '<no_period_provided>' or '<since_launch>' etc."
        ),
        SkillParameter(
            name="metrics",
            is_multi=True,
            constrained_to="metrics",
            description="test"
        ),
        SkillParameter(
            name="limit_n",
            constrained_values=["<no_limit>", "5"],
            description=" if 'show all' is specified please use the <no_limit> token"
        ),
        SkillParameter(
            name="year_over_year_variance",
            constrained_to=None,
            constrained_values=["Y/Y", "None"],
            description="When set to 'None', the analysis will show actuals compared to the budgets. When set to 'Y/Y' the analysis will show actual compared to previous year spending."
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters",
            description=""
        ),
        SkillParameter(
            name="outlook",
            constrained_to=None,
            constrained_values=["Y", "None"],
            description="When user asks for outlook, please set this to Y, else None"
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=MAX_PROMPT
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=INSIGHT_PROMPT
        ),
        SkillParameter(
            name="summary_tab_layout",
            parameter_type="visualization",
            description="Summary Viz Layout",
            default_value=SUMMARY_TEMPLATE
        ),
        SkillParameter(
            name="dim_tab_layout",
            parameter_type="visualization",
            description="Dim Viz Layout",
            default_value=DIM_TEMPLATE
        )
    ]
)
def run_expense_drivers(parameters: SkillInput):
    param_dict = {"periods": [], "metrics": None, "limit_n": "5", "year_over_year_variance": None,
                  "other_filters": [], "calculated_metric_filters": None, "outlook": None}
    print(f"Skill received following parameters: {parameters.arguments}")
    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    # hard code so the LLM cannot change
    # param_dict['limit_n'] = 5

    env = SimpleNamespace(**param_dict)
    ExpenseDriversCoreTemplateParameterSetup(env=env)
    env.ed = ExpenseDriversCore.from_env(env=env)
    skill_output = env.ed.run_from_env()

    tables = env.ed.get_display_tables()
    charts = env.ed.get_display_charts()
    chart_metrics = skill_output["target_metric_columns"]
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.ed.parameter_display_information.items()]

    dim_tab_titles = env.ed.dim_tab_titles
    dim_tab_names = env.ed.dim_tab_names
    viz_dim_tab_map = env.ed.viz_dim_tab_map

    insights_dfs = [env.ed.df_notes, env.ed.expense_facts, env.ed.subject_facts]
    #followups = env.ed.get_suggestions()

    viz, insights, final_prompt = render_layout(tables, charts, env.ed.title, env.ed.subtitle, insights_dfs,
                                                env.ed.warning_message, chart_metrics, env.ed.metric_props,
                                                parameters.arguments.max_prompt, parameters.arguments.insight_prompt,
                                                growth_type=env.growth_type,
                                                summary_tab_layout=parameters.arguments.summary_tab_layout,
                                                dim_tab_layout=parameters.arguments.dim_tab_layout,
                                                dim_tab_titles=dim_tab_titles,
                                                dim_tab_names=dim_tab_names,
                                                viz_dim_tab_map=viz_dim_tab_map
                                                )

    exported_data = []
    for name, table_obj in tables.items():
        df = table_obj['df']
        df = df.rename(columns={"": "      "})
        df = df.set_index(df.columns[0])
        exported_data.append(ExportData(name=name, data=df))

    return SkillOutput(
        final_prompt=final_prompt,
        visualizations=viz,
        parameter_display_descriptions=param_info,
        followup_questions=env.ed.suggestions,
        export_data=exported_data
    )

def render_layout(
        tables: Dict[str, pd.DataFrame], 
        charts: Dict[str, pd.DataFrame], 
        title: str, 
        subtitle: str, 
        insights_dfs, 
        warnings: str,
        chart_columns: List[str],
        metric_props: Dict[str, Dict],
        max_prompt: str, 
        insights_prompt: str,
        summary_tab_layout,
        dim_tab_layout,
        dim_tab_titles,
        dim_tab_names,
        viz_dim_tab_map,
        growth_type: str = None
        ):
    height = 80
    # template = jinja2.Template(TEMPLATE)
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insights_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})

    print("****** insight_template ******")
    print(insight_template)

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)
    viz_list = []

    variance_rules = {
        VARIANCE_DIFF_COL.lower() : {
            "highlight_font": {
                "value_rules": {
                    "upper": {
                        "value": 0,
                        "style": {"color": "green"},
                    },
                    "lower": {
                        "value": 0,
                        "style": {"color": "red"},
                    }
                }
            }
        },
        VARIANCE_DIFF_PER_COL.lower(): {
            "highlight_font": {
                "value_rules": {
                    "upper": {
                        "value": 0,
                        "style": {"color": "green"},
                    },
                    "lower": {
                        "value": 0,
                        "style": {"color": "red"},
                    }
                }
            },
            "highlight_cells": {
                "actual_fte": {
                    "value_rules": {
                        "upper": {
                            "value": OVER_UNDER_VARIANCE_THRESHOLD.get('actual_fte').get('high'),
                            "style": {"backgroundColor": "lightgreen"},
                        },
                        "lower": {
                            "value": OVER_UNDER_VARIANCE_THRESHOLD.get('actual_fte').get('low'),
                            "style": {"backgroundColor": "lightyellow"},
                        }
                    }
                },
                "actual_dollars": {
                    "value_rules": {
                        "upper": {
                            "value": OVER_UNDER_VARIANCE_THRESHOLD.get('actual_dollars').get('high'),
                            "style": {"backgroundColor": "lightgreen"},
                        },
                        "lower": {
                            "value": OVER_UNDER_VARIANCE_THRESHOLD.get('actual_dollars').get('low'),
                            "style": {"backgroundColor": "lightyellow"},
                        }
                    }
                }
            }
        }
    }

    growth_rules = {
         GROWTH_DIFF_COL.lower(): {
            "highlight_font": {
                "value_rules": {
                    "upper": {
                        "value": 0,
                        "style": {"color": "green"},
                    },
                    "lower": {
                        "value": 0,
                        "style": {"color": "red"},
                    }
                }
            }
        },
        GROWTH_DIFF_PER_COL.lower(): {
            "highlight_font": {
                "value_rules": {
                    "upper": {
                        "value": 0,
                        "style": {"color": "green"},
                    },
                    "lower": {
                        "value": 0,
                        "style": {"color": "red"},
                    }
                }
            }
        }
    }

    ar_utils = ArUtils()
    shared_fn = SharedFn()

    def get_dim_level_totals(chart_data: List[Dict] | Dict, level_to_retrieve: int, curr_level: int = 0) -> List[Dict]:
        '''
        Recursively retrieves a flattened list of the data at the level specified.

        :param chart_data: List[Dict] - The nested data to retrieve the level from
        :param level_to_retrieve: int - The level to retrieve
        :param curr_level: int - The current level

        :return: List[Dict] - The flattened data at the level specified
        '''
        
        data_row = []

        if isinstance(chart_data, dict):
            chart_data = [chart_data]

        for row in chart_data:
            if curr_level == level_to_retrieve:
                data_row.append(row)
            else:
                if 'group' in row:
                    data_row = data_row + get_dim_level_totals(row['group'], level_to_retrieve, curr_level + 1)

        return data_row
    
    def get_chart_obj(chart_title: str, dim_level_data: List[Dict], chart_columns: List[str], format_obj: Dict) -> Dict:
        '''
        Creates a chart object for the given chart title and dimension level data.

        :param chart_title: str - The title of the chart
        :param dim_level_data: List[Dict] - The dimension level data
        :param chart_columns: List[str] - The columns to display in the chart
        :param format_obj: Dict - The format object

        :return: Dict - The chart object
        '''
                
        chart_data = [
          {
            "name": metric,
            "data": []
          } for metric in chart_columns
        ]
        chart_categories = []

        for dim_row in dim_level_data:
            chart_categories.append(dim_row['data'][0])
            for i, _ in enumerate(chart_columns):
                chart_data[i]['data'].append(dim_row['data'][i+1]['value'])

        return {
            "title": chart_title,
            "categories": chart_categories,
            "data": chart_data,
            "value_format": format_obj["value_format"],
            "point_y_format": format_obj["point_y_format"]
        }
    
    for name, table_obj in tables.items():
        
        table = table_obj['df']
        agg_set = table_obj.get('agg_set', None)

        if not growth_type:
            cell_styling_rules = variance_rules
        else:
            cell_styling_rules = growth_rules

        # Convert DataFrame rows to hierarchical structure
        if dim_tab_map[name.lower()] == Tabs.Summary:
            output = ExpenseDriversHelpers.transform_visual_content(cell_styling_rules, table, include_collapsable_rows=False)
        else:
            output = ExpenseDriversHelpers.transform_visual_content(cell_styling_rules, table, 1)

        chart_objs = []
        chart_output = charts.get(name.lower(), {}).get("df", None)
        if dim_tab_map[name.lower()] != Tabs.Summary and chart_output is not None and agg_set is not None:

            metric = chart_output['Metric_Name'].unique()[0] # used for formatting
            format_obj = ar_utils.python_to_highcharts_format(shared_fn.get_metric_prop(metric, metric_props).get('fmt'))

            top_level_rows = LEVELS_FOR_DATA_PULL[agg_set.lower()]

            pl_top_level_idx = top_level_rows.index('pl') if 'pl' in top_level_rows else None
            dim_top_level_idx = top_level_rows.index(name.lower()) if name.lower() in top_level_rows else 0

            indentation_col = chart_output.columns[0]
            chart_output = chart_output[[indentation_col] + chart_columns]

            chart_data = ExpenseDriversHelpers.transform_visual_content({}, chart_output, 0)

            if pl_top_level_idx is not None:
                
                pl_data = get_dim_level_totals(chart_data, pl_top_level_idx)

                for pl_row in pl_data:

                    chart_objs.append(get_chart_obj(
                      chart_title=pl_row['data'][0],
                      dim_level_data=get_dim_level_totals(pl_row, dim_top_level_idx),
                      chart_columns=chart_columns,
                      format_obj=format_obj
                    ))

            else:

                chart_objs.append(get_chart_obj(
                  chart_title="",
                  dim_level_data=get_dim_level_totals(chart_data, dim_top_level_idx),
                  chart_columns=chart_columns,
                  format_obj=format_obj
                ))

        viz_template = dim_tab_layout
        dim_tab_title = ""
        if 'summary' in name.lower():
            viz_template = summary_tab_layout
        else:
            # populate dim title
            if dim_tab_titles:
                dim_tab_title = dim_tab_titles.get(name.lower(), name)

        tab_vars = {
            "OverviewTitle": title,
            "OverviewFilterSubTitle": subtitle,
            "OverviewInsights": insights,
            "DimTitle": dim_tab_title,
        }
        table_vars = get_table_layout_vars(table)

        col_def = table_vars.get('col_defs')

        col_def = [c for c in col_def if c.get('name').lower() != 'metric_name']

        styled_table_vars = {
            'table_data': output,
            'col_defs': col_def,
        }

        input_vars = {**tab_vars, **styled_table_vars}

        chart_vars = {}
        if chart_objs:
            primary_chart = chart_objs[0]

            chart_vars = {
                "primary_categories": primary_chart.get('categories'),
                "primary_data": primary_chart.get('data'),
                "primary_chart_title": primary_chart.get('title'),
                "primary_chart_data_label": {
                    "enabled": True,
                    "format": primary_chart.get('point_y_format'),
                    "style": {
                        "fontSize": "11px"
                    }
                },
                "primary_yaxis": {
                    "title": {
                        "text": ""
                    },
                    "labels": {
                        "format": primary_chart.get('value_format')
                    },
                    "visible": False
                },
                "primary_plot_options": {
                    "bar": {
                        "dataLabels": {
                            "enabled": True,
                            "format": primary_chart.get('point_y_format'),
                            "style": {
                                "fontSize": "11px"
                            }
                        }
                    }
                }
            }

            if len(chart_objs) > 1:
                secondary_chart = chart_objs[1]
                chart_vars.update({
                    "secondary_categories": secondary_chart.get('categories'),
                    "secondary_data": secondary_chart.get('data'),
                    "secondary_chart_title": secondary_chart.get('title'),
                    "hide_secondary_chart": False,
                    "secondary_chart_data_label": {
                        "enabled": True,
                        "format": secondary_chart.get('point_y_format'),
                        "style": {
                            "fontSize": "11px"
                        }
                    },
                    "secondary_yaxis": {
                        "title": {
                            "text": ""
                        },
                        "labels": {
                            "format": secondary_chart.get('value_format')
                        },
                        "visible": False
                    },
                    "secondary_plot_options": {
                        "bar": {
                            "dataLabels": {
                                "format": secondary_chart.get('point_y_format'),
                                "enabled": True,
                                "style": {
                                    "fontSize": "11px"
                                }
                            }
                        }
                    }
                })
            else:
                chart_vars.update({
                    "secondary_categories": [],
                    "secondary_data": [],
                    "hide_secondary_chart": True
                })

        tab_name = name
        if dim_tab_names:
            tab_name = dim_tab_names.get(tab_name.lower(), name)

        tab_name = viz_dim_tab_map.get(tab_name, tab_name)

        rendered = wire_layout(json.loads(viz_template, strict=False), {**input_vars, **chart_vars})
        viz_list.append(SkillVisualization(title=tab_name, layout=rendered))

    return viz_list, insights, max_response_prompt


if __name__ == '__main__':

    test_case_1_variance = {'metrics': ["actual_dollars", "actual_fte"],
                   'periods': ["Q3 2024"], "limit_n": 5, "other_filters": [{"dim": "subpackage", "op": "=", "val": ["Subscriptions"]}]}

    test_case_2_variance = {'metrics': ["actual_dollars", "budgeted_dollars", "actual_fte", "budgeted_fte"],
                            'periods': ["YTD Q2 2024"], "limit_n": 5,
                            'other_filters': [{"dim": "packages", "op": "=", "val": ["travel"]}]}

    test_case_3_variance = {'metrics': ["actual_fte"],
                            'periods': ["Q1 2024"],
                            'other_filters': [{"dim": "function", "op": "=", "val": ["sales"]}]}

    test_case_4_variance = {'metrics': ["actual_dollars"],
                            'periods': ["Q1 2024"], "limit_n": "<no_limit>",
                            "other_filters": [    {"dim": "subpackage", "op": "=", "val": ["Voice Mobile"]}]}

    test_case_5_variance = {'metrics': ["actual_dollars", "actual_fte"],
                            'periods': ["Q4 2024"],
                            "other_filters": [{"dim": "function", "op": "=", "val": ["Sales"]}]}

    test_case_6_variance_ytd = {
  "periods": [
    "ytd nov 2024"
  ],
  "metrics": [
    "actual_dollars"
  ]
}

    test_case_7_variance_ytd = {
        "periods": [
            "ytd feb 2024"
        ],
        "metrics": [
            "actual_fte"
        ]
    }

    test_case_1_growth = {'metrics': ["actual_dollars"], "limit_n": 5,
                            'periods': ["Q4 2024"], "growth_type": "Y/Y"}

    test_case_2_growth = {'metrics': ["budgeted_dollars"],
                            'periods': ["Q4 2024"],
                            'other_filters': [{"dim": "subpackage", "op": "=", "val": ["Subscriptions"]}], "growth_type": "Y/Y"}

    test_case_3_growth = {'metrics': ["actual_fte"],
                            'periods': ["Q4 2024"], "growth_type": "Y/Y",
                            'other_filters': [{"dim": "function", "op": "=", "val": ["Sales"]}]}
    #F_Sales

    test_case_4_fte_growth = {'metrics': ["actual_fte"],
                          'periods': ["Q4 2024"], "growth_type": "Y/Y",
                          'other_filters': []}
    test_case_1_no_data = {
  "periods": [
    "ytd q1 2024"
  ],
  "metrics": [
    "actual_dollars"
  ],
  "other_filters": [
    {
      "val": [
        "fix pension"
      ],
      "dim": "subpackage",
      "op": "="
    }
  ]
}
    test_case_country_package_variance = {
  "periods": [
    "ytd feb 2024"
  ],
  "metrics": [
    "actual_dollars"
  ],
  "outlook": "Y",
  "other_filters": [
    {
      "val": [
        "china"
      ],
      "dim": "country",
      "op": "="
    },
    {
      "val": [
        "Miscellaneous Expense"
      ],
      "dim": "subpackage",
      "op": "="
    }
  ]
}


    test_case_variance_no_limit = {
  "periods": [
    "ytd 2024"
  ],
  "metrics": [
    "actual_dollars"
  ],
  "growth_type": "None",
  "limit_n": "5"
}

    skill_input: SkillInput = run_expense_drivers.create_input(
                arguments=test_case_1_variance
    )
    out = run_expense_drivers(skill_input)
    preview_skill(run_expense_drivers, out)
