# Import Statement
import os
import pandas as pd
import sqlite3
from selection import *
from file_handling_docker import *
from functions import *
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px
import math


print("Program: Folder_File_Analysis_Dashboard")
print("Release: 0.0.0")
print("Date: 2021-11-05")
print("Author: Brian Neely")
print()
print()
print("WIP - A program that that analyzes a directory of files and folders its metadata.")
print()
print()

# Import Data
meta_data = open_unknown_csv('out.csv', ',')

# Make log of bytes
meta_data['file_size_bytes_log'] = meta_data['file_size_bytes'].apply(lambda x: 0 if x == 0 else math.log(x, 2))

# Initialize Dash
app = dash.Dash()

# Dash Layout
app.layout = html.Div([
    dcc.Graph(id="graph"),
    html.P("File Size:"),
    # dcc.Slider(id="size", min=0, max=int(meta_data['file_size_bytes'].max()), value=0,
    #            marks={0: '0', int(meta_data['file_size_bytes'].max()): str(meta_data['file_size_bytes'].max())}),
    dcc.RangeSlider(
        id='size_range',
        min=0,
        max=int(meta_data['file_size_bytes_log'].max()),
        step=1,
        value=[0, int(meta_data['file_size_bytes_log'].max())],
        marks={
            0: {'label': '0 B', 'style': {'color': '#77b0b1'}},
            math.log(1024, 2): {'label': '1 KiB'},
            math.log(1024*1024, 2): {'label': '1 MiB'},
            math.log(1024*1024*1024, 2): {'label': '1 GiB'},
            math.log(1024*1024*1024*1024, 2): {'label': '1 TiB', 'style': {'color': '#f50'}}
        }
    ),
    html.Div(id='output-container-range-slider')
])


@app.callback(
    Output("graph", "figure"),
    [Input("size_range", "value")])
def display_color(size_range):
    meta_data_size_fltrd = meta_data[meta_data['file_size_bytes_log'].between(size_range[0], size_range[1])]
    print(meta_data_size_fltrd)
    fig = px.histogram(meta_data_size_fltrd,
                       x='file_size_bytes_log',
                       labels={'file_size_bytes_log': 'Log2(File Size)'})
    return fig


@app.callback(
    dash.dependencies.Output('output-container-range-slider', 'children'),
    [dash.dependencies.Input('size_range', 'value')])
def update_output(value):
    return 'You have selected {start} <-> {end}'.format(start=str(sizeof_fmt(2 ** value[0])), end=str(sizeof_fmt(2 ** value[1])))

# Make Histogram
# fig = px.histogram(meta_data, x="file_size_bytes")
# fig.show()

app.run_server()

