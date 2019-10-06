'''
Create a website to display earthquake location information.
Calls database with queries through query_mysql.py
Additionally functions can be found in display_tools.py

modeled after code from Andrew Bierbaum:
    https://github.com/andrewbierbaum/InsightProject

Filename: display.py
Cohort: Insight Data Engineering SEA '19C
Name: Carl Ulberg
'''


#import mysql.connector
#from mysql.connector import Error
#import pandas as pd
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
#import plotly.graph_objs as go
#import plotly.express as px
import numpy as np
import os
#from datetime import datetime as dt
import dash_daq as daq
import query_mysql as q
from display_tools import convert_value_to_month, \
                            convert_value_to_date, \
                            make_eq_dict


# define location methods to display
METHODS = [0, 1, 2]

# define and setup app
external_stylesheets = ['https://codepen.io/anon/pen/mardKv.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True

# open connection to database
conn = q.ReadConnector(os.environ['MYSQL_DB'])
print('Successfully connected to database...')

# read station locations, since they don't change
sta = q.get_stations(conn)

# define interactive portions of the app
dateslider = html.Div([
    html.P(children='Select date'),
    daq.Slider(
        id='month-slider',
        min=0, max=599, value=250,
        marks={'0': '1970-01', '240': '1990-01', '480': '2010-01'}
    ),
    html.Br(),
    html.Div(id='slider-output')
], style={'text-align': 'center', 'margin': '0 auto'})

eq_plot = html.Div(
    id='eqplot',
    style={'width': '50%', 'display': 'inline-block'}
)

# the main graph program, which simply links to the tabs below when selected
app.layout = html.Div([
    html.H1('EQlocator', style={'text-align': 'center'}),
    html.H2('Re-evaluating historical records to find faults',
        style={'text-align': 'center'}),
    dcc.Tabs(id='tabs-navigation', value='location-plots', children=[
        dcc.Tab(label='Location Plots', value='location-plots'),
        dcc.Tab(label='Station Information', value='station-info'),
    ]),
    html.Div(id='eq-plots')
])


# These are the main and secondary pages in tab 1 and 2
@app.callback(Output('eq-plots', 'children'),
              [Input('tabs-navigation', 'value')])
def render_content(tab):
    if tab == 'location-plots':
        return html.Div(
            id='dark-theme-feature',
            children=[
                html.Br(),
                dateslider,
                html.Br(),
                eq_plot
            ]
        )

    # building the station info tab
    elif tab == 'station-info':
        return html.Div(
            id='sta-tables',
            children=[
                html.H2(children='Which stations are used the most?',
                    style={'text-align': 'center'}),
                html.P(children='Stations with fewer observations may require improvements',
                    style={'text-align': 'center'}),
                html.H2(children='Least used stations',
                    style={'text-align': 'center'}),
                dash_table.DataTable(
                    style_data={'whiteSpace': 'normal'},
                    css=[{'selector': '.dash-cell div.dash-cell-value', 'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}],
                    id='most-used-table',
                    columns=[{'name': i, 'id': i} for i in sta.columns],
                    data=sta.sort_values(
                            by=['count', 'sta'],
                            ascending=[True, False]
                        ).head(5).to_dict('records')
                ),
                html.Br(),
                html.H2(children='Most used stations',style={'text-align': 'center'}),
                dash_table.DataTable(
                    style_data={'whiteSpace': 'normal'},
                    css=[{'selector': '.dash-cell div.dash-cell-value', 'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}],
                    id='most-used-table',
                    columns=[{'name': i, 'id': i} for i in sta.columns],
                    data=sta.sort_values(
                            by=['count', 'sta'],
                            ascending=[False, True]
                        ).head(5).to_dict('records')
                )
            ]
        )


@app.callback(
    dash.dependencies.Output('slider-output', 'children'),
    [dash.dependencies.Input('month-slider', 'value')])
def update_text_output(value):
    mo_tmp = convert_value_to_month(value)
    return 'Displaying earthquakes during {}'.format(mo_tmp)


@app.callback(
    dash.dependencies.Output('eqplot', 'children'),
    [dash.dependencies.Input('month-slider', 'value')])
def update_earthquake_plot(value):
    date = convert_value_to_date(value)

    data2 = [dict(
            type='scattergeo',
            locationmode='USA-states',
            lon=sta['Station_Longitude'],
            lat=sta['Station_Latitude'],
            text=sta['sta'],
            mode='markers',
            name='Stations',
            marker=dict(
                size=8,
                opacity=0.8,
                symbol='triangle-up'
            )
    )]

    layout2 = dict(
        title='Earthquake Locations',
        geo=dict(
            resolution=50,
            projection=dict(
                type='conic equidistant',
            ),
            showland=True,
            landcolor='rgb(250,250,250)',
            subunitcolor='rgb(207,207,207)',
            countrycolor='rgb(217,217,217)',
            countrywidth=0.5,
            subunitwidth=0.5,
            center=dict(
                lon=sta['Station_Longitude'].mean(),
                lat=sta['Station_Latitude'].mean()
            ),
            lonaxis=dict(range=[
                np.min(sta['Station_Longitude'])-.6,
                np.max(sta['Station_Longitude']+.6)
            ]),
            lataxis=dict(range=[
                np.min(sta['Station_Latitude'])-.5,
                np.max(sta['Station_Latitude']+.5)
            ])
        ),
        uirevision=True
    )

    for method in METHODS:
        df = q.get_locations(date, method, conn)
        df = df.sample(frac=0.03)
        data2.append(make_eq_dict(df, method))

    fig2 = dict(data=data2, layout=layout2)
    return dcc.Graph(figure=fig2)


# this is the actual server call
if __name__ == '__main__':
    app.run_server(debug=True)
    q.ReadDisconnect(conn)
