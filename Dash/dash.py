import mysql.connector
from mysql.connector import Error
import pandas as pd
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.express as px
import numpy as np


host = 'ec2-52-32-192-161.us-west-2.compute.amazonaws.com'
db = 'final' # large2, tmp4, tmp5
data_table1 = 'table5' # table1, small0, [table3,table14]
data_table2 = 'table4'
sta_table = 'statmp2'
user = 'user'
pw = 'pw'

# define query to only select a single 'day': ~40,000 events
read_data_sql2='select * from {} where Event_id between 1980010100000000 and 1980010200000000;'.format(data_table1)
#read_data_sql='select * from {};'.format(data_table1)
read_data_sql1='select * from {} where Event_id between 1980010100000000 and 1980010200000000;'.format(data_table2)

conn = mysql.connector.connect(host=host,
                                database=db,
                                user=user,
                                password=pw,
                                use_pure=True)

cur = conn.cursor()
# read in data
df1 = pd.read_sql(read_data_sql1, conn)
df2 = pd.read_sql(read_data_sql2, conn)
#sta = pd.read_sql("SELECT * FROM {}".format(sta_table), conn)
sta = pd.read_sql("SELECT * FROM tmp5.statmp2".format(sta_table), conn)

# close the sql
conn.close()

# randomly sample the data
df1 = df1.sample(n=1000)
df2 = df2.sample(n=1000)
#print("df has {} rows".format(df.count()))

#setup dark mode
colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

def make_eq_dict(df, method):
    data_dict = dict(
        type = 'scattergeo',
        locationmode = 'USA-states',
        lon = df['Longitude'],
        lat = df['Latitude'],
        mode = 'markers',
        name = 'Algorithm {}'.format(method),
        marker = dict(
            size = 4,
            opacity = 0.8
        )
    )
    return data_dict

# set up location figure
data2 = [
    dict(
        type = 'scattergeo',
        locationmode = 'USA-states',
        lon = sta['Station_Longitude'],
        lat = sta['Station_Latitude'],
        text = sta['sta'],
        mode = 'markers',
        name = 'Stations',
        marker = dict(
            size = 8,
            opacity = 0.8,
            symbol = 'triangle-up'
        )
    ),
    make_eq_dict(df1, 1),
    make_eq_dict(df2, 2)
#    dict(
#        type = 'scattergeo',
#        locationmode = 'USA-states',
#        lon = df['Longitude'],
#        lat = df['Latitude'],
#        mode = 'markers',
#        name = 'Algorithm 1',
#        marker = dict(
#            size = 4,
#            opacity = 0.8
#        )
#    )
#    dict(
#        type = 'scattergeo',
#        locationmode = 'USA-states',
#        lon = df2['Longitude'],
#        lat = df2['Latitude'],
#        mode = 'markers',
#        name = 'Algorithm 2',
#        marker = dict(
#        size = 4,
#        opacity = 0.8
#        )
#    )
]

layout2 = dict(
    title = 'Earthquake Locations',
    geo = dict(
#        scope='usa',
        resolution=50,
        projection=dict(
            type='conic equidistant',
#            rotation=dict(
#                lon=[-120],
#                lat=[45]
#            )
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
        lonaxis=dict(
            range=[
                np.min(sta['Station_Longitude'])-.6,
                np.max(sta['Station_Longitude']+.6)
            ]
        ),
        lataxis=dict(
            range=[
                np.min(sta['Station_Latitude'])-.5,
                np.max(sta['Station_Latitude']+.5)
            ]
        )
    ),
    
               
)

fig2 = dict(data=data2, layout=layout2)

#fig3 = go.Figure(
#    go.Scattermapbox(
#fig3 = px.scatter_mapbox(
#        sta,
#        lat='Station_Longitude',
#        lon='Station_Latitude',
#        mode='markers',
#        marker=go.scattermapbox.Marker(size=9),
#
##    )
#)
#fig3.update_layout(
#    mapbox_style="stamen-terrain"
#)

#setting up dash
external_stylesheets = ['https://codepen.io/anon/pen/mardKv.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#the main graph program, which simply links to the tabs below when selected
app.layout = html.Div([
    html.H1('EQlocator',style={'text-align': 'center'}),
    html.H2('Re-evaluating historical records to find faults',style={'text-align': 'center'}),
    dcc.Tabs(id="tabs-navigation", value='location-plots', children=[
        dcc.Tab(label='Location Plots', value='location-plots'),
        dcc.Tab(label='Station Information', value='station-info'),
    ]),
    html.Div(id='eq-plots')
])

#These are the main and secondary pages in tab 1 and 2
@app.callback(Output('eq-plots', 'children'),
              [Input('tabs-navigation', 'value')])
def render_content(tab):
    if tab == 'location-plots':
        return html.Div(id='dark-theme-feature',children=[
#            html.H2(children='Earthquake locations',style={'text-align': 'center'}),
#	html.Div(children ='user data',id='text-context'),
            #html.Div(children='''Hover and Click to Display User Comments''',style={'text-align': 'center','font-size': 22}),
            html.Br(),
#            html.Div([
#            #builds the Location graph
#                dcc.Graph(
#                    id='Earthquake Locations',
#                    animate = True,
#                    figure={
#                    'data': [
#                            {'x': sta['Station_Longitude'], 'y': sta['Station_Latitude'], 'type': 'scatter', 'mode': 'markers', 'name': 'Stations', 'marker': {'symbol': 'triangle-up'}},
#                            {'x': df['Longitude'], 'y': df['Latitude'], 'type': 'scatter', 'mode': 'markers', 'name': 'algorithm 1'},
#                    ],
#                    'layout': {
#                        'hovermode': 'closest',
#                        'yaxis': {'title': {'text': 'Latitude'}},
#                        'xaxis': {'title': {'text': 'Longitude'}}
#                    }
#                    }
#                )],style={'width': '50%', 'display': 'inline-block'}),
#            html.Br(),
            html.Div([
                dcc.Graph(id='eqlocs2', figure=fig2)
#                dcc.Graph(id='eqlocs2', figure=fig3)
            ],style={'width': '50%', 'display': 'inline-block'}),

#these are the click and mouseover textboxes
#            html.H4(children ='Hover over data to quick view',id='HackerNews-hover-text',style={'width': '49%','display':'inline-block','vertical-align': 'top','height':'175px','overflow': 'hidden','border':'groove', 'border-radius': '5px','margin-top': '5px','margin-bottom':'5px'}),
#            html.H4(children ='Hover over data to quick view',id='Reddit-hover-text',style={'width': '49%','display':'inline-block','vertical-align': 'top','height':'175px','overflow': 'hidden','border':'groove', 'border-radius': '5px','margin-top': '5px','margin-bottom':'5px'}),
#            html.H4(children ='Click data to Select',id='HackerNews-text',style={'width': '49%','display':'inline-block','vertical-align': 'top','height':'200px','overflow-y': 'scroll','border':'groove', 'border-radius': '5px','margin-top': '5px','margin-bottom':'5px'}),
#            html.H4(children ='Click data to Select',id='Reddit-text',style={'width': '49%','display':'inline-block','vertical-align': 'top','height':'200px','overflow-y': 'scroll','border':'groove', 'border-radius': '5px','margin-top': '5px','margin-bottom':'5px'}),
    ])

    #building the algorithms tab
    elif tab == 'station-info':
        return html.Div(id = 'sta-tables',children =[
            html.H2(children='Which stations are used the most?',style={'text-align': 'center'}),
            html.P(children='Stations with fewer observations may require improvements',style={'text-align': 'center'}),
            html.H2(children='Least used stations',style={'text-align': 'center'}),
            dash_table.DataTable(
                style_data={'whiteSpace': 'normal'},
                css=[{'selector': '.dash-cell div.dash-cell-value', 'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}],
                id='most-used-table',
                columns=[{'name': i, 'id': i} for i in sta.columns],
                data=sta.sort_values(by=['count', 'sta'], ascending=[True, False]) \
                .head(5).to_dict('records')
                ),
            html.Br(),
            html.H2(children='Most used stations',style={'text-align': 'center'}),
            dash_table.DataTable(
                style_data={'whiteSpace': 'normal'},
                css=[{'selector': '.dash-cell div.dash-cell-value', 'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}],
                id='most-used-table',
                columns=[{'name': i, 'id': i} for i in sta.columns],
                data=sta.sort_values(by=['count', 'sta'], ascending=[False, True]) \
                .head(5).to_dict('records')
            )]
        )

#this is the actual server call
if __name__ == '__main__':
    app.run_server(debug=True)
