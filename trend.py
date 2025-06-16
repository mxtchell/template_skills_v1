from __future__ import annotations
from types import SimpleNamespace

from skill_framework import SkillVisualization, skill, SkillParameter, SkillInput, SkillOutput, ParameterDisplayDescription
from skill_framework.preview import preview_skill
from skill_framework.skills import ExportData
from skill_framework.layouts import wire_layout

from ar_analytics import AdvanceTrend, TrendTemplateParameterSetup, ArUtils
from ar_analytics.defaults import trend_analysis_config, default_trend_chart_layout, default_table_layout, get_table_layout_vars

import jinja2
import logging
import json

default_ppt_trend_chart_layout = """
{
	"layoutJson": {
		"type": "Canvas",
		"rows": 90,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "100%"
		},
		"children": [
			{
				"name": "Header0",
				"type": "Header",
				"row": 10,
				"column": 12,
				"width": 105,
				"height": 7,
				"text": "Pasta Sales Have Trended Downward, With Signs of Recent Growth",
				"style": {
					"fontSize": "40px",
					"fontWeight": "bold",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#2563eb",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "center"
				},
				"extraStyles": ""
			},
			{
				"name": "HighchartsChart0",
				"type": "HighchartsChart",
				"row": 40,
				"column": 12,
				"width": 138,
				"height": 45,
				"children": "",
				"minHeight": "270px",
				"style": {
					"border": "solid #DDD 1.5px",
					"padding": "10px",
					"borderRadius": "11.911px"
				},
				"options": {
					"chart": {
						"type": "line",
						"polar": false
					},
					"title": {
						"text": ""
					},
					"xAxis": {
						"categories": [
							"Q1 22",
							"Q2 22",
							"Q3 22",
							"Q4 22",
							"Q1 23",
							"Q2 23",
							"Q3 23",
							"Q4 23",
							"Q1 24",
							"Q2 24"
						]
					},
					"yAxis": {
						"title": {
							"text": ""
						},
						"labels": {
							"format": "${value}M"
						},
						"max": 4
					},
					"legend": {
						"align": "center",
						"verticalAlign": "bottom",
						"layout": "horizontal",
						"enabled": true
					},
					"plotOptions": {
						"series": {
							"marker": {
								"enabled": false,
								"states": {
									"hover": {
										"enabled": false
									}
								}
							}
						}
					},
					"series": [
						{
							"name": "Product A",
							"data": [
								0.8,
								1,
								1.2,
								1.5,
								1.7,
								1.9,
								2.2,
								2.4,
								2.6,
								2.8
							],
							"color": "#8A75CA"
						},
						{
							"name": "Product B",
							"data": [
								0.2,
								0.5,
								0.9,
								1.1,
								1,
								0.8,
								1.2,
								1.3,
								1.1,
								1.3
							],
							"color": "#9AC89C"
						},
						{
							"name": "Product C",
							"data": [
								0.1,
								0.3,
								0.6,
								0.9,
								1.2,
								1.4,
								1.6,
								1.8,
								2,
								2.1
							],
							"color": "#E07D3B"
						},
						{
							"name": "Product D",
							"data": [
								0.2,
								0.4,
								0.7,
								1,
								1.1,
								1.2,
								1.3,
								1.5,
								1.6,
								1.7
							],
							"color": "#3B86E0"
						}
					]
				},
				"maxHeight": "270px",
				"hidden": false
			},
			{
				"name": "Paragraph1",
				"type": "Markdown",
				"row": 18,
				"column": 12,
				"width": 120,
				"height": 15,
				"text": "* First item\\n* Second item\\n* Third item\\n",
				"style": {
					"fontSize": "22px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Paragraph0",
				"type": "Paragraph",
				"row": 16,
				"column": 12,
				"width": 70,
				"height": 2,
				"text": "Enter Paragraph Text",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Header1",
				"type": "Header",
				"row": 37,
				"column": 12,
				"width": 120,
				"height": 3,
				"text": "Enter Header Text",
				"style": {
					"fontSize": "25",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			}
		]
	},
	"inputVariables": [
		{
			"name": "exec_summary",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Paragraph1",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "headline",
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
			"name": "sub_headline",
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
			"name": "absolute_x_axis_categories",
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
			"name": "absolute_series",
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
			"name": "absolute_y_axis",
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
			"name": "absolute_metric_name",
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
			"name": "hide_absolute_series_name",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header1",
					"fieldName": "hidden"
				}
			]
		}
	]
}
"""

default_ppt_trend_growth_chart_layout = """
{
	"layoutJson": {
		"type": "Canvas",
		"rows": 90,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "100%"
		},
		"children": [
			{
				"name": "Header0",
				"type": "Header",
				"row": 10,
				"column": 12,
				"width": 105,
				"height": 7,
				"text": "Pasta Sales Have Trended Downward, With Signs of Recent Growth",
				"style": {
					"fontSize": "40px",
					"fontWeight": "bold",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#2563eb",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "center"
				},
				"extraStyles": ""
			},
			{
				"name": "HighchartsChart0",
				"type": "HighchartsChart",
				"row": 40,
				"column": 12,
				"width": 138,
				"height": 45,
				"children": "",
				"minHeight": "270px",
				"style": {
					"border": "solid #DDD 1.5px",
					"padding": "10px",
					"borderRadius": "11.911px"
				},
				"options": {
					"chart": {
						"type": "line",
						"polar": false
					},
					"title": {
						"text": ""
					},
					"xAxis": {
						"categories": [
							"Q1 22",
							"Q2 22",
							"Q3 22",
							"Q4 22",
							"Q1 23",
							"Q2 23",
							"Q3 23",
							"Q4 23",
							"Q1 24",
							"Q2 24"
						]
					},
					"yAxis": {
						"title": {
							"text": ""
						},
						"labels": {
							"format": "${value}M"
						},
						"max": 4
					},
					"legend": {
						"align": "center",
						"verticalAlign": "bottom",
						"layout": "horizontal",
						"enabled": true
					},
					"plotOptions": {
						"series": {
							"marker": {
								"enabled": false,
								"states": {
									"hover": {
										"enabled": false
									}
								}
							}
						}
					},
					"series": [
						{
							"name": "Product A",
							"data": [
								0.8,
								1,
								1.2,
								1.5,
								1.7,
								1.9,
								2.2,
								2.4,
								2.6,
								2.8
							],
							"color": "#8A75CA"
						},
						{
							"name": "Product B",
							"data": [
								0.2,
								0.5,
								0.9,
								1.1,
								1,
								0.8,
								1.2,
								1.3,
								1.1,
								1.3
							],
							"color": "#9AC89C"
						},
						{
							"name": "Product C",
							"data": [
								0.1,
								0.3,
								0.6,
								0.9,
								1.2,
								1.4,
								1.6,
								1.8,
								2,
								2.1
							],
							"color": "#E07D3B"
						},
						{
							"name": "Product D",
							"data": [
								0.2,
								0.4,
								0.7,
								1,
								1.1,
								1.2,
								1.3,
								1.5,
								1.6,
								1.7
							],
							"color": "#3B86E0"
						}
					]
				},
				"maxHeight": "270px",
				"hidden": false
			},
			{
				"name": "Paragraph1",
				"type": "Markdown",
				"row": 18,
				"column": 12,
				"width": 120,
				"height": 15,
				"text": "* First item\\n* Second item\\n* Third item\\n",
				"style": {
					"fontSize": "22px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Paragraph0",
				"type": "Paragraph",
				"row": 16,
				"column": 12,
				"width": 70,
				"height": 2,
				"text": "Enter Paragraph Text",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Header1",
				"type": "Header",
				"row": 37,
				"column": 12,
				"width": 120,
				"height": 3,
				"text": "Enter Header Text",
				"style": {
					"fontSize": "25",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Header2",
				"type": "Header",
				"row": 85,
				"column": 12,
				"width": 100,
				"height": 3,
				"text": "Enter Header Text",
				"style": {
					"fontSize": "20px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			}
		]
	},
	"inputVariables": [
		{
			"name": "headline",
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
			"name": "sub_headline",
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
			"name": "exec_summary",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Paragraph1",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "growth_x_axis_categories",
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
			"name": "growth_series",
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
			"name": "growth_y_axis",
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
			"name": "growth_metric_name",
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
			"name": "warning",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header2",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "hide_growth_warning",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header2",
					"fieldName": "hidden"
				}
			]
		}
	]
}
"""

default_ppt_trend_diff_chart_layout = """
{
	"layoutJson": {
		"type": "Canvas",
		"rows": 90,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "100%"
		},
		"children": [
			{
				"name": "Header0",
				"type": "Header",
				"row": 10,
				"column": 12,
				"width": 105,
				"height": 7,
				"text": "Pasta Sales Have Trended Downward, With Signs of Recent Growth",
				"style": {
					"fontSize": "40px",
					"fontWeight": "bold",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#2563eb",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "center"
				},
				"extraStyles": ""
			},
			{
				"name": "HighchartsChart0",
				"type": "HighchartsChart",
				"row": 40,
				"column": 12,
				"width": 138,
				"height": 45,
				"children": "",
				"minHeight": "270px",
				"style": {
					"border": "solid #DDD 1.5px",
					"padding": "10px",
					"borderRadius": "11.911px"
				},
				"options": {
					"chart": {
						"type": "line",
						"polar": false
					},
					"title": {
						"text": ""
					},
					"xAxis": {
						"categories": [
							"Q1 22",
							"Q2 22",
							"Q3 22",
							"Q4 22",
							"Q1 23",
							"Q2 23",
							"Q3 23",
							"Q4 23",
							"Q1 24",
							"Q2 24"
						]
					},
					"yAxis": {
						"title": {
							"text": ""
						},
						"labels": {
							"format": "${value}M"
						},
						"max": 4
					},
					"legend": {
						"align": "center",
						"verticalAlign": "bottom",
						"layout": "horizontal",
						"enabled": true
					},
					"plotOptions": {
						"series": {
							"marker": {
								"enabled": false,
								"states": {
									"hover": {
										"enabled": false
									}
								}
							}
						}
					},
					"series": [
						{
							"name": "Product A",
							"data": [
								0.8,
								1,
								1.2,
								1.5,
								1.7,
								1.9,
								2.2,
								2.4,
								2.6,
								2.8
							],
							"color": "#8A75CA"
						},
						{
							"name": "Product B",
							"data": [
								0.2,
								0.5,
								0.9,
								1.1,
								1,
								0.8,
								1.2,
								1.3,
								1.1,
								1.3
							],
							"color": "#9AC89C"
						},
						{
							"name": "Product C",
							"data": [
								0.1,
								0.3,
								0.6,
								0.9,
								1.2,
								1.4,
								1.6,
								1.8,
								2,
								2.1
							],
							"color": "#E07D3B"
						},
						{
							"name": "Product D",
							"data": [
								0.2,
								0.4,
								0.7,
								1,
								1.1,
								1.2,
								1.3,
								1.5,
								1.6,
								1.7
							],
							"color": "#3B86E0"
						}
					]
				},
				"maxHeight": "270px",
				"hidden": false
			},
			{
				"name": "Paragraph1",
				"type": "Markdown",
				"row": 18,
				"column": 12,
				"width": 120,
				"height": 15,
				"text": "* First item\\n* Second item\\n* Third item\\n",
				"style": {
					"fontSize": "22px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Paragraph0",
				"type": "Paragraph",
				"row": 16,
				"column": 12,
				"width": 70,
				"height": 2,
				"text": "Enter Paragraph Text",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Header1",
				"type": "Header",
				"row": 37,
				"column": 12,
				"width": 120,
				"height": 3,
				"text": "Enter Header Text",
				"style": {
					"fontSize": "25",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Header2",
				"type": "Header",
				"row": 85,
				"column": 12,
				"width": 100,
				"height": 3,
				"text": "Enter Header Text",
				"style": {
					"fontSize": "20px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			}
		]
	},
	"inputVariables": [
		{
			"name": "headline",
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
			"name": "sub_headline",
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
			"name": "exec_summary",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Paragraph1",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "difference_x_axis_categories",
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
			"name": "difference_series",
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
			"name": "difference_y_axis",
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
			"name": "difference_metric_name",
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
			"name": "warning",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header2",
					"fieldName": "text"
				}
			]
		},
		{
			"name": "hide_growth_warning",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Header2",
					"fieldName": "hidden"
				}
			]
		}
	]
}
"""

default_ppt_table_layout = """

{
	"layoutJson": {
		"type": "Canvas",
		"rows": 90,
		"columns": 160,
		"rowHeight": "1.11%",
		"colWidth": "0.625%",
		"gap": "0px",
		"style": {
			"backgroundColor": "#ffffff",
			"width": "100%",
			"height": "100%"
		},
		"children": [
			{
				"name": "Header0",
				"type": "Header",
				"row": 10,
				"column": 12,
				"width": 105,
				"height": 7,
				"text": "Pasta Sales Have Trended Downward",
				"style": {
					"fontSize": "40px",
					"fontWeight": "bold",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#2563eb",
					"backgroundColor": "#ffffff",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb",
					"alignItems": "center"
				},
				"extraStyles": ""
			},
			{
				"name": "Paragraph1",
				"type": "Markdown",
				"row": 18,
				"column": 12,
				"width": 120,
				"height": 15,
				"text": "* First item\\n* Second item\\n* Third item\\n",
				"style": {
					"fontSize": "22px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "Paragraph0",
				"type": "Paragraph",
				"row": 16,
				"column": 12,
				"width": 70,
				"height": 2,
				"text": "Enter Paragraph Text",
				"style": {
					"fontSize": "15px",
					"fontWeight": "normal",
					"textAlign": "left",
					"verticalAlign": "start",
					"color": "#000000",
					"border": "none",
					"textDecoration": "none",
					"writingMode": "horizontal-tb"
				}
			},
			{
				"name": "DataTable0",
				"type": "DataTable",
				"row": 32,
				"column": 12,
				"width": 135,
				"height": 50,
				"columns": [
					{
						"name": "Column 1"
					},
					{
						"name": "Column 2"
					},
					{
						"name": "Column 3"
					},
					{
						"name": "Column 4"
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
				]
			}
		]
	},
	"inputVariables": [
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
			"name": "data",
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
			"name": "headline",
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
			"name": "sub_headline",
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
			"name": "exec_summary",
			"isRequired": false,
			"defaultValue": null,
			"targets": [
				{
					"elementName": "Paragraph1",
					"fieldName": "text"
				}
			]
		}
	]
}
"""

RUNNING_LOCALLY = False

logger = logging.getLogger(__name__)

@skill(
    name=trend_analysis_config.name,
    llm_name=trend_analysis_config.llm_name,
    description=trend_analysis_config.description,
    capabilities=trend_analysis_config.capabilities,
    limitations=trend_analysis_config.limitations,
    example_questions=trend_analysis_config.example_questions,
    parameter_guidance=trend_analysis_config.parameter_guidance,
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
            constrained_to="metrics"
        ),
        SkillParameter(
            name="limit_n",
            description="limit the number of values by this number",
            default_value=10
        ),
        SkillParameter(
            name="breakouts",
            is_multi=True,
            constrained_to="dimensions",
            description="breakout dimension(s) for analysis."
        ),
        SkillParameter(
            name="time_granularity",
            is_multi=False,
            constrained_to="date_dimensions",
            description="time granularity provided by the user. only add if explicitly stated by user."
        ),
        SkillParameter(
            name="growth_type",
            constrained_to=None,
            constrained_values=["Y/Y", "P/P", "None"],
            description="Growth type either Y/Y, P/P, or None"
        ),
        SkillParameter(
            name="other_filters",
            constrained_to="filters"
        ),
        SkillParameter(
            name="max_prompt",
            parameter_type="prompt",
            description="Prompt being used for max response.",
            default_value=trend_analysis_config.max_prompt
        ),
        SkillParameter(
            name="insight_prompt",
            parameter_type="prompt",
            description="Prompt being used for detailed insights.",
            default_value=trend_analysis_config.insight_prompt
        ),
        SkillParameter(
            name="table_viz_layout",
            parameter_type="visualization",
            description="Table Viz Layout",
            default_value=default_table_layout
        ),
        SkillParameter(
            name="chart_viz_layout",
            parameter_type="visualization",
            description="Chart Viz Layout",
            default_value=default_trend_chart_layout
        ),
        SkillParameter(
            name="abs_chart_ppt_layout",
            parameter_type="visualization",
            description="abs chart slide Viz Layout",
            default_value=default_ppt_trend_chart_layout
        ),
        SkillParameter(
            name="growth_chart_ppt_layout",
            parameter_type="visualization",
            description="growth chart slide Viz Layout",
            default_value=default_ppt_trend_growth_chart_layout
        ),
        SkillParameter(
            name="diff_chart_ppt_layout",
            parameter_type="visualization",
            description="diff chart slide Viz Layout",
            default_value=default_ppt_trend_diff_chart_layout
        ),
        SkillParameter(
            name="table_ppt_export_viz_layout",
            parameter_type="visualization",
            description="table slide Viz Layout",
            default_value=default_ppt_table_layout
        )
    ]
)
def trend(parameters: SkillInput):
    print(f"Skill received following parameters: {parameters.arguments}")
    param_dict = {"periods": [], "metrics": None, "limit_n": 10, "breakouts": [], "growth_type": None, "other_filters": [], "time_granularity": None}

    # Update param_dict with values from parameters.arguments if they exist
    for key in param_dict:
        if hasattr(parameters.arguments, key) and getattr(parameters.arguments, key) is not None:
            param_dict[key] = getattr(parameters.arguments, key)

    env = SimpleNamespace(**param_dict)
    TrendTemplateParameterSetup(env=env)
    env.trend = AdvanceTrend.from_env(env=env)
    df = env.trend.run_from_env()
    param_info = [ParameterDisplayDescription(key=k, value=v) for k, v in env.trend.paramater_display_infomation.items()]
    tables = [env.trend.display_dfs.get("Metrics Table")]

    insights_dfs = [env.trend.df_notes, env.trend.facts, env.trend.top_facts, env.trend.bottom_facts]

    charts = env.trend.get_dynamic_layout_chart_vars()

    viz, slides, insights, final_prompt = render_layout(charts,
                                                tables,
                                                env.trend.title,
                                                env.trend.subtitle,
                                                insights_dfs,
                                                env.trend.warning_message,
                                                parameters.arguments.max_prompt,
                                                parameters.arguments.insight_prompt,
                                                parameters.arguments.table_viz_layout,
                                                parameters.arguments.chart_viz_layout,
                                                parameters.arguments.abs_chart_ppt_layout,
                                                parameters.arguments.growth_chart_ppt_layout,
                                                parameters.arguments.diff_chart_ppt_layout,
                                                parameters.arguments.table_ppt_export_viz_layout)

    return SkillOutput(
        final_prompt=final_prompt,
        narrative=None,
        visualizations=viz,
        ppt_slides=slides,
        parameter_display_descriptions=param_info,
        followup_questions=[],
        export_data=[ExportData(name="Metrics Table", data=tables[0])]
    )

def render_layout(charts, tables, title, subtitle, insights_dfs, warnings, max_prompt, insight_prompt, table_viz_layout, chart_viz_layout, abs_chart_ppt_layout, growth_chart_ppt_layout, diff_chart_ppt_layout, table_ppt_export_viz_layout):
    facts = []
    for i_df in insights_dfs:
        facts.append(i_df.to_dict(orient='records'))

    insight_template = jinja2.Template(insight_prompt).render(**{"facts": facts})
    max_response_prompt = jinja2.Template(max_prompt).render(**{"facts": facts})

    # adding insights
    ar_utils = ArUtils()
    insights = ar_utils.get_llm_response(insight_template)

    tab_vars = {"headline": title if title else "Total",
                "sub_headline": subtitle or "Trend Analysis",
                "hide_growth_warning": False if warnings else True,
                "exec_summary": insights if insights else "No Insight.",
                "warning": warnings}

    viz = []
    slides = []
    for name, chart_vars in charts.items():
        chart_vars["footer"] = f"*{chart_vars['footer']}" if chart_vars.get('footer') else "No additional info."
        rendered = wire_layout(json.loads(chart_viz_layout), {**tab_vars, **chart_vars})
        viz.append(SkillVisualization(title=name, layout=rendered))
        try:
            abs_slide = wire_layout(json.loads(abs_chart_ppt_layout), {**tab_vars, **chart_vars})
            slides.append(abs_slide)
            if "hide_growth_chart" not in chart_vars or not chart_vars["hide_growth_chart"]:
                growth_slide = wire_layout(json.loads(growth_chart_ppt_layout), {**tab_vars, **chart_vars})
                slides.append(growth_slide)
                diff_slide = wire_layout(json.loads(diff_chart_ppt_layout), {**tab_vars, **chart_vars})
                slides.append(diff_slide)
        except Exception as e:
            logger.error(f"Error rendering chart ppt slide: {e}")

    table_vars = get_table_layout_vars(tables[0])
    table = wire_layout(json.loads(table_viz_layout), {**tab_vars, **table_vars})
    viz.append(SkillVisualization(title="Metrics Table", layout=table))

    if table_ppt_export_viz_layout is not None:
        try: 
            table_slide = wire_layout(json.loads(table_ppt_export_viz_layout), {**tab_vars, **table_vars})
            slides.append(table_slide)
        except Exception as e:
            logger.error(f"Error rendering table ppt slide: {e}")
    else:
        slides.append(table)

    return viz, slides, insights, max_response_prompt

if __name__ == '__main__':
    # Create a custom slides visualization layout
    custom_slides_layout = default_trend_chart_layout  # You can customize this if needed

    skill_input: SkillInput = trend.create_input(arguments={
        'metrics': ["sales", "volume"],
        'periods': ["2021", "2022"],
        'growth_type': "Y/Y",
        "other_filters": [{"dim": "brand", "op": "=", "val": ["barilla"]}],
        "slides_viz_layout": custom_slides_layout
    })
    out = trend(skill_input)
    preview_skill(trend, out)