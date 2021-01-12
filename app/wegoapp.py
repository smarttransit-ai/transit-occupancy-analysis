# %%
import dash
import flask
import os
from random import randint
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import plotly.express as px
import dataextract
from dash.dependencies import Input, Output
from plotly import graph_objs as go
from plotly.graph_objs import *
from datetime import datetime as dt
from datetime import time as tt
import os,sys,resource
import dask.dataframe as dd
from fastparquet import ParquetFile
 
#resource.setrlimit(resource.RLIMIT_AS, (1e9, 1e9))  
# Data preparation Code. Uncomment as required

# df = dataextract.decompress_pickle('nashville_bus_occupancy_dashboard-dubey.pbz2')
# df['time_day_seconds']=df.swifter.apply(lambda row: row.timeofday.hour*60*60+row.timeofday.minute*60+row.timeofday.second, axis=1)
# sdf = dd.from_pandas(df, npartitions=10)
# custom= {'occupancy': 'max', 'board_count': 'sum', 'triptime': 'first','datetime':'first','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
# max_occupancy_board_by_trip_day=sdf.groupby(['trip_id','route_id','direction_desc','month','year','date']).agg(custom).reset_index()
# custom= {'occupancy': 'max', 'board_count': 'sum','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
# max_occupancy_board_by_stop_day=sdf.groupby(['route_id','trip_id','stop_id','direction_desc','stop_name','stop_lat','stop_lon','month','year','date']).agg(custom).reset_index()
# max_occupancy_board_by_route_day=sdf.groupby(['route_id','direction_desc','month','year','date']).agg(custom).reset_index()
# max_occupancy_board_by_trip_day.to_parquet('data/nashville/max_occupancy_board_by_trip_day.parquet', engine='fastparquet')
# max_occupancy_board_by_stop_day.to_parquet('data/nashville/max_occupancy_board_by_stop_day.parquet', engine='fastparquet')
# max_occupancy_board_by_route_day.to_parquet('data/nashville/max_occupancy_board_by_route_day.parquet', engine='fastparquet')
# print(max_occupancy_board_by_trip_day.head())
# print(max_occupancy_board_by_trip_day.dtypes)
# print(max_occupancy_board_by_trip_day.datetime.min().compute())
# sys.exit(0)
 

#set the server and global parameters
server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
app = dash.Dash(__name__, server=server,meta_tags=[{"name": "viewport", "content": "width=device-width"}])
app.title='Nashville Occupancy Dashboard'
latInitial=36.16228
lonInitial=-86.774372
mapbox_access_token = "pk.eyJ1Ijoidmlzb3ItdnUiLCJhIjoiY2tkdTZteWt4MHZ1cDJ4cXMwMnkzNjNwdSJ9.-O6AIHBGu4oLy3dQ3Tu2XA"
px.set_mapbox_access_token(mapbox_access_token)

#load all required dataframes as dask
#sdf = dd.read_parquet('data/nashville/occupancy.parquet', engine='fastparquet')

max_occupancy_board_by_trip_day=dd.read_parquet('data/nashville/max_occupancy_board_by_trip_day.parquet', engine='fastparquet')
max_occupancy_board_by_stop_day=dd.read_parquet('data/nashville/max_occupancy_board_by_stop_day.parquet', engine='fastparquet')
max_occupancy_board_by_route_day=dd.read_parquet('data/nashville/max_occupancy_board_by_route_day.parquet', engine='fastparquet')

print (max_occupancy_board_by_trip_day.head(n=10))

#sys.exit(0)

#find all routes and directions.
startdate=max_occupancy_board_by_trip_day.datetime.min().compute() 
enddate=max_occupancy_board_by_trip_day.datetime.max().compute() 
print(enddate,startdate)
routes=max_occupancy_board_by_trip_day.route_id.unique().compute() 
directions=max_occupancy_board_by_trip_day.direction_desc.unique().compute()

del max_occupancy_board_by_trip_day
del max_occupancy_board_by_stop_day
del max_occupancy_board_by_route_day

#setup the app frameworks and main layout
# Layout of Dash App
app.layout = html.Div(
    children=[
        html.Div(
            className="row",
            children=[
                # Column for user controls
                html.Div(
                    className="four columns div-user-controls",
                    children=[
                        dcc.Markdown('''# WeGo Transit Occupancy'''),
#                         html.Div(className="container",children=[html.P("Choose Statistics",className="five columns",style={"font-size": "1.4rem"}),
#   ],style={'display': 'inline-block'}),
                 
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                #html.P("""Select Start Date """,style={'text-align': 'left' ,'font-weight':'bold'}),
                                dcc.Markdown('''## Select Start Date'''),
                                dcc.DatePickerSingle(
                                    id="date-picker",
                                    min_date_allowed=startdate.date(),
                                    max_date_allowed=enddate.date(),
                                    initial_visible_month=startdate.date(),
                                    date=startdate.date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[                                
                                dcc.Markdown('''## Select End Date'''),
                                dcc.DatePickerSingle(
                                    id="date-picker-end",
                                    min_date_allowed=startdate.date(),
                                    max_date_allowed=enddate.date(),
                                    initial_visible_month=enddate.date(),
                                    date=enddate.date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[                                
                                dcc.Markdown('''## Select Bus Routes'''),
                                dcc.Checklist(
                                        options=[
                                             {'label': '{}'.format(str(i)), 'value': '{}'.format(str(i))} for i in routes
                                        ],                                        
                                        id='bus-routes' ,
                                         labelStyle={'display': 'inline-block'} ,
                                    ),                                                         
                            ],
                        ),
                        # Change to side-by-side for mobile layout
                        html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        dcc.Markdown('''## Filter Data By Month'''),
                                        #html.P("""Select from the list to filter data by month.""",style={'text-align': 'left' ,'font-weight':'bold'}),
                                        # Dropdown to select times
                                        dcc.Checklist(
                                            id="month-selector",
                                            options=[
                                                
                                                    {'label': 'Jan', 'value': '1'},
                                                    {'label': 'Feb', 'value': '2'},
                                                    {'label': 'Mar', 'value': '3'},
                                                    {'label': 'Apr', 'value': '4'},
                                                    {'label': 'May', 'value': '5'},
                                                    {'label': 'June', 'value': '6'},
                                                    {'label': 'July', 'value': '7'},
                                                    {'label': 'Aug', 'value': '8'},
                                                    {'label': 'Sep', 'value': '9'},
                                                    {'label': 'Oct', 'value': '10'},
                                                    {'label': 'Nov', 'value': '11'},
                                                    {'label': 'Dec', 'value': '12'},
                                                
                                                
                                            ],
                                            
                                            labelStyle={'display': 'inline-block'}                                         
                                        )
                                    ],
                                ),
                    html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        dcc.Markdown('''## Filter Data By Day of Week'''),
                                        #html.P("""Select from the list to filter data by month.""",style={'text-align': 'left' ,'font-weight':'bold'}),
                                        # Dropdown to select times
                                        dcc.Checklist(
                                            id="day-selector",
                                            options=[
                                                
                                                    {'label': 'Mon', 'value': '0'},
                                                    {'label': 'Tue', 'value': '1'},
                                                    {'label': 'Wed', 'value': '2'},
                                                    {'label': 'Thur', 'value': '3'},
                                                    {'label': 'Fri', 'value': '4'},
                                                    {'label': 'Sat', 'value': '5'},
                                                    {'label': 'Sun', 'value': '6'},
                                            ],
                                            
                                            labelStyle={'display': 'inline-block'}                                         
                                        )
                                    ],
                                ),
                        html.Div(style={'display': 'none'},
                                    className="div-for-dropdown",                                    
                                    children=[
                                        dcc.Markdown('''## Filter by Pattern'''),
                                        dcc.Checklist( id='pattern-selector',
                                                options=[                                             
                                                    {'label': 'Weekday', 'value': 'weekday'},
                                                    {'label': 'Weekend', 'value': 'weekend'},
                                                    ],
                                                value=['weekday','weekend'],
                                                labelStyle={'display': 'inline-block'} ,                                                     
                                            ),                                        
                                    ]
                                ),
                        html.Div(className="div-for-dropdown",                                    
                                    children=[
                                        dcc.Markdown('''## Filter by Direction'''),
                                        dcc.Checklist( id='direction-selector',
                                                options=[
                                                {'label': '{}'.format(str(i)), 'value': '{}'.format(str(i))} for i in directions
                                                 ],
                                                
                                                labelStyle={'display': 'inline-block'} ,                                                     
                                            ),                                        
                                    ]
                                ),
                        html.Div(className="div-for-dropdown",
                                    children=[
                                    dcc.Markdown('''## Filter Data By Trip Start Time'''),
                                    #html.P("""Select Time Range""",style={'text-align': 'left' ,'font-weight':'bold'}),
                                    dcc.RangeSlider(
                                        id='time-slider',
                                        min=0,
                                        max=24,
                                        step=0.25,
                                        value=[0, 24],
                                        marks={i: '{}:00'.format(str(i).zfill(2)) for i in range(0, 25,4)},
                                    ),
                                    
                                                ]
                                    ),
                        html.Div(style={'text-align': 'center','display':'none'},children=[dcc.RadioItems( id='occupancy-basis',
                                          options=[                                             
                                              {'label': 'Show Occupancy', 'value': 'occupancy'},
                                              {'label': 'Show Boardings', 'value': 'boarding'},],
                                        labelStyle={'display': 'inline-block'} ,     
                                        value='occupancy',style={'text-align': 'center'},
                                    )]),
                                    html.Div(style={'text-align': 'center','display':'none'}, children=[html.P('Incidents',id='incident-text',style={'text-align': 'left','font-weight': 'bold'}),
                                                       html.P('Months',id='month-text',style={'text-align': 'left','font-weight': 'bold'}),
                                                       html.P('Time',id='time-text',style={'text-align': 'left','font-weight': 'bold'}),
                                                       html.P('Response',id='response-text',style={'text-align': 'left','font-weight': 'bold'}),
                                                      ],
                                    )
                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="eight columns div-for-charts bg-grey",
                    children=[
                        html.Div(className="container-fluid",children=[
                        dcc.Loading(id="loading-icon1",children=[dcc.Graph(id="map-graph"),],type='default'),]),
                        #html.Div(className="div-for-dropdown", children=[html.P(id='heatmap-text',style={'text-align': 'center'})]),     
                        html.Div(
                            className="row",children=[
                        html.Div(
                            className="six columns textright", 
                            children=[
                                #html.P("""Select Start Date """,style={'text-align': 'left' ,'font-weight':'bold'}),
                                dcc.RadioItems( id='configure-statistics',
                                          options=[                                             
                                              {'label': 'Show Occupancy', 'value': 'occupancy'},
                                              {'label': 'Show Boardings', 'value': 'board_count'},],
                                        labelStyle={'display': 'inline-block'} ,     
                                        value='occupancy',
                                    )
                              
                            ],
                        ),
                        html.Div(className="six columns textleft", children=[dcc.RadioItems( id='histogram-trip',
                                          options=[                                             
                                              {'label': 'Show By Stops', 'value': 'stops','disabled':True},
                                              {'label': 'Show By Trips', 'value': 'trips'},
                                              {'label': 'Show By Routes', 'value': 'routes'}],
                                        labelStyle={'display': 'inline-block'} ,     
                                        value='routes',
                                    )])
                                ],

                        ),                                
                        html.Div(className="container-fluid",children=[                  
                        dcc.Loading(id="loading-icon2", children=[dcc.Graph(id="histogram"),],type='default'),
                        dcc.Markdown(id="warning-text",children=['# Please select one route for showing stop information.'],style={'text-align': 'center','display':'none'})
                        ]
                        ),
                       
                    ],
                ),
            ],
        ),
         html.Div(className="container-fluid ",children=[dcc.Markdown(""),dcc.Markdown('Site designed by [ScopeLab](http://scopelab.ai/) starting from [the Uber Ride Demo from Plotly](https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-uber-rides-demo). Data source: Nashville WeGo. Funding for this work has been provided by the National Science Foundation under awards [CNS-2029950](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2029950) and [CNS-2029952](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2029952). Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.',id='footer'),]),     
    ]
)



def return_seconds(time):
    return time.hour*3600+time.minute*60+time.second

#sys.exit(0)
#max_occupancy_board_by_trip_day['trip_time']=max_occupancy_board_by_trip_day['triptime'].dt.time
#max_occupancy_board_by_trip_day['trip_time']=max_occupancy_board_by_trip_day['triptime'].astype(pd.Timestamp) max_occupancy_board_by_trip_day['time_day_seconds']>return_seconds(starttime)
#print(max_occupancy_board_by_trip_day.dtypes)
#result= max_occupancy_board_by_trip_day [  (max_occupancy_board_by_trip_day['time_day_seconds']>return_seconds(starttime)) & (max_occupancy_board_by_trip_day['datetime']>st)   ]
#print(result.head())




@app.callback(
     Output('map-graph', 'figure'),
    [Input("date-picker", "date"),
    Input("date-picker-end", "date"),
    #Input("map-graph-radius", "value"),
    Input('bus-routes', 'value'),
    Input("month-selector", "value"),
    Input("direction-selector", "value"),
    Input("day-selector", "value"),
    Input("occupancy-basis","value"),    
    Input("time-slider", "value"),Input("configure-statistics","value")]
)
def update_map(start_date, end_date,  busroutes, months, directions,days,occupancykind,timerange,statisticskind):
    #setup time condition
    #max_occupancy_board_by_trip_day=dd.read_parquet('data/nashville/max_occupancy_board_by_trip_day.parquet', engine='fastparquet')
    max_occupancy_board_by_stop_day=dd.read_parquet('data/nashville/max_occupancy_board_by_stop_day.parquet', engine='fastparquet')
    #max_occupancy_board_by_route_day=dd.read_parquet('data/nashville/max_occupancy_board_by_route_day.parquet', engine='fastparquet')
    timemin,timemax=timerange        
    hourmax=int(timemax)
    hourmin=int(timemin)
    if hourmax==24:
        endtime= return_seconds(tt(23,59,59))    
    else:
        minutesmax=int(60*(timemax-hourmax))
        endtime=return_seconds(tt(hourmax,minutesmax,0))
    minutesmin=int(60*(timemin-hourmin))
    starttime= return_seconds(tt(hourmin,minutesmin,0))
    if days is None or len(days)==0:
        weekday_condition2=(True)
        
    else:
        #weekday_condition= ((max_occupancy_board_by_trip_day['dayofweek']).isin(days))
        weekday_condition2 = ((max_occupancy_board_by_stop_day['dayofweek']).isin(days))
    #timecondition = ((max_occupancy_board_by_trip_day['time_day_seconds']>starttime) & (max_occupancy_board_by_trip_day['time_day_seconds']<endtime))
    timecondition2 = ((max_occupancy_board_by_stop_day['time_day_seconds']>starttime) & (max_occupancy_board_by_stop_day['time_day_seconds']<endtime))
    #direction condition
    if directions is None or len(directions)==0:
       # direction_condition=(True)
        direction_condition2=(True)
    else:
       # direction_condition= ((max_occupancy_board_by_trip_day['direction_desc']).isin(directions))    
        direction_condition2=((max_occupancy_board_by_stop_day['direction_desc']).isin(directions)) 

    if months is None or len(months)==0:
        month_condition2=True
    else:
        month_condition2=((max_occupancy_board_by_stop_day['month'].isin(months)))
    

    if busroutes is None or len(busroutes)==0:
        #route_condition=(True)
        route_condition2=(True)
    else:
        #print(busroutes)
        #print (max_occupancy_board_by_trip_day['route_id'].dtype)
       # route_condition  = ((max_occupancy_board_by_trip_day['route_id']).isin(busroutes))
        route_condition2 = ((max_occupancy_board_by_stop_day['route_id']).isin(busroutes))

    #datecondtition=((max_occupancy_board_by_trip_day['date'] >= start_date) & (max_occupancy_board_by_trip_day['date'] <= end_date))
    datecondtition2 = ((max_occupancy_board_by_stop_day['date'] >= start_date) & (max_occupancy_board_by_stop_day['date'] <= end_date))
    if(statisticskind=="board_count"):
        finalstatistics="MeanTotalBoardingsByDayPerRoute"    
        result_max_occupancy_board_by_stop_day=max_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (month_condition2) &(datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','date'])[statisticskind].sum().reset_index()
        result_max_occupancy_board_by_stop_day=result_max_occupancy_board_by_stop_day.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon'])[statisticskind].mean().reset_index().compute()
    else:
        finalstatistics="MeanMaxOccupancyDayPerRoute"
        result_max_occupancy_board_by_stop_day=max_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (month_condition2) &(datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon'])[statisticskind].mean().reset_index().compute()
    result_max_occupancy_board_by_stop_day[finalstatistics]=result_max_occupancy_board_by_stop_day[statisticskind]
    fig = px.scatter_mapbox(result_max_occupancy_board_by_stop_day,
                                lat="stop_lat",
                                lon="stop_lon",
                                color=finalstatistics,
                                #animation_frame='date',
                                #animation_group='stop_id',
                                range_color=[0, int(20)],
                                #color_continuous_midpoint=meanresult.board_count.median(),
                                text='stop_name',
                                mapbox_style="light",
                                #color_continuous_scale=px.colors.cyclical.IceFire,
                                zoom=10) 
    fig.update_layout(
        autosize=True,
        margin=go.layout.Margin(l=0, r=35, t=0, b=0),
        plot_bgcolor="#1E1E1E",
        #paper_bgcolor="#1E1E1E",
        #title_text= title,
        hoverlabel=dict(font=dict(size=12))
    )
    del result_max_occupancy_board_by_stop_day
    del max_occupancy_board_by_stop_day
    return  fig


@app.callback(
     Output('histogram-trip', 'options'),
    [Input('bus-routes', 'value')]
)
def update_histogram_options(busroutes):
    #options1=[{'label': 'Show By Stops', 'value': 'stops','disabled':True},
                                            #  {'label': 'Show By Trips', 'value': 'trips'},
                                            #  {'label': 'Show By Routes', 'value': 'routes'}]

    #options2=[{'label': 'Show By Stops', 'value': 'stops','disabled':False},
                                            #  {'label': 'Show By Trips', 'value': 'trips'},
                                            #  {'label': 'Show By Routes', 'value': 'routes'}],
    if busroutes is None or len(busroutes)!=1:
        return [{'label': 'Show By Trips', 'value': 'trips'},{'label': 'Show By Routes', 'value': 'routes'}]
    else:
        return [{'label': 'Show By Stops', 'value': 'stops'},{'label': 'Show By Trips', 'value': 'trips'},{'label': 'Show By Routes', 'value': 'routes'}]

    

@app.callback(
     Output('histogram-trip', 'value'),
    [Input('histogram-trip', 'options')]
)
def set_histogram_value (available_options):
    i=len(available_options)
    return available_options[i-1]['value']

# %%
@app.callback(
    Output('histogram', 'figure'),
    [Input("date-picker", "date"),
    Input("date-picker-end", "date"),
    #Input("map-graph-radius", "value"),
    Input('bus-routes', 'value'),
    Input("month-selector", "value"),
    Input("direction-selector", "value"),
    Input("day-selector", "value"),
    Input("occupancy-basis","value"),    
    Input("time-slider", "value"),Input("histogram-trip","value"),Input("configure-statistics","value")]
)
def update_histogram(start_date, end_date,  busroutes, months, directions,days,occupancykind,timerange,histogramtrip,statisticskind):
    #setup time condition
    
    if histogramtrip=='stops' and (busroutes is None or len (busroutes) >1):
        return {}
    
    
    max_occupancy_board_by_trip_day=dd.read_parquet('data/nashville/max_occupancy_board_by_trip_day.parquet', engine='fastparquet')
    max_occupancy_board_by_stop_day=dd.read_parquet('data/nashville/max_occupancy_board_by_stop_day.parquet', engine='fastparquet')
    max_occupancy_board_by_route_day=dd.read_parquet('data/nashville/max_occupancy_board_by_route_day.parquet', engine='fastparquet')
    timemin,timemax=timerange        
    hourmax=int(timemax)
    hourmin=int(timemin)
    if hourmax==24:
        endtime= return_seconds(tt(23,59,59))    
    else:
        minutesmax=int(60*(timemax-hourmax))
        endtime=return_seconds(tt(hourmax,minutesmax,0))
    minutesmin=int(60*(timemin-hourmin))
    starttime= return_seconds(tt(hourmin,minutesmin,0))
    if days is None or len(days)==0:
        weekday_condition=(True)
        weekday_condition2=weekday_condition
        weekday_condition3=weekday_condition
    else:
        weekday_condition= ((max_occupancy_board_by_trip_day['dayofweek']).isin(days))
        weekday_condition2 = ((max_occupancy_board_by_stop_day['dayofweek']).isin(days))
        weekday_condition3=((max_occupancy_board_by_route_day['dayofweek']).isin(days))
    timecondition = ((max_occupancy_board_by_trip_day['time_day_seconds']>starttime) & (max_occupancy_board_by_trip_day['time_day_seconds']<endtime))
    timecondition2 = ((max_occupancy_board_by_stop_day['time_day_seconds']>starttime) & (max_occupancy_board_by_stop_day['time_day_seconds']<endtime))
    timecondition3=((max_occupancy_board_by_route_day['time_day_seconds']>starttime) & (max_occupancy_board_by_route_day['time_day_seconds']<endtime))
    
    #month condition

    if months is None or len(months)==0:
        month_condition=True
        month_condition2=True
        month_condition3=True
    else:
        month_condition = ((max_occupancy_board_by_trip_day['month'].isin(months)))
        month_condition2=((max_occupancy_board_by_stop_day['month'].isin(months)))
        month_condition3=((max_occupancy_board_by_route_day['month'].isin(months)))
 
    
    #direction condition
    if directions is None or len(directions)==0:
        direction_condition=(True)
        direction_condition2=direction_condition
        direction_condition3=direction_condition
    else:
        direction_condition= ((max_occupancy_board_by_trip_day['direction_desc']).isin(directions))    
        direction_condition2=((max_occupancy_board_by_stop_day['direction_desc']).isin(directions)) 
        direction_condition3=((max_occupancy_board_by_route_day['direction_desc']).isin(directions)) 

    if busroutes is None or len(busroutes)==0:
        route_condition=(True)
        route_condition2=route_condition
        route_condition3=route_condition
    else:
        route_condition  = ((max_occupancy_board_by_trip_day['route_id']).isin(busroutes))
        route_condition2 = ((max_occupancy_board_by_stop_day['route_id']).isin(busroutes))
        route_condition3 = ((max_occupancy_board_by_route_day['route_id']).isin(busroutes))

    datecondtition=((max_occupancy_board_by_trip_day['date'] >= start_date) & (max_occupancy_board_by_trip_day['date'] <= end_date))
    datecondtition2 = ((max_occupancy_board_by_stop_day['date'] >= start_date) & (max_occupancy_board_by_stop_day['date'] <= end_date))
    datecondtition3 = ((max_occupancy_board_by_route_day['date'] >= start_date) & (max_occupancy_board_by_route_day['date'] <= end_date))
    if histogramtrip=='trips':
        result_max_occupancy_board_by_trip_day = max_occupancy_board_by_trip_day [  (route_condition) & (direction_condition) & (datecondtition) & (month_condition) &(timecondition) & (weekday_condition) ].compute().sort_values(['time_day_seconds'])
        if(len(result_max_occupancy_board_by_trip_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_occupancy_board_by_trip_day,labels={'trip_start_time':'Trips','board_count':'Total Boardings'}, x="trip_start_time", y="board_count",color="route_id",hover_data=['route_id'])
        else:
            fig2 = px.box(result_max_occupancy_board_by_trip_day,labels={'trip_start_time':'Trips','occupancy':'Max Daily Occupancy'}, x="trip_start_time", y="occupancy",color="route_id",hover_data=['route_id'])
        fig2.update_layout(
            autosize=True,
            bargap=0.1,
            bargroupgap=0,
            xaxis_type='category',
            barmode="stack",
            margin=go.layout.Margin(l=0, r=35, t=0, b=0),
            plot_bgcolor="#31302F",
            font=dict(color="white"),
            paper_bgcolor="#31302F",
            yaxis=dict(                                    
                #range=[0, occupancymax+2],
                showticklabels=True,
                showgrid=False,
                fixedrange=True,
                rangemode="nonnegative",
                zeroline=False,
            ),
            xaxis_tickangle=-45,
            hoverlabel=dict(font=dict(size=12))
        )
        del result_max_occupancy_board_by_trip_day        
        #return  fig2
    elif histogramtrip=='stops':
        result_max_occupancy_board_by_stop_day=max_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (month_condition2) &  (datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].compute()
        if(len(result_max_occupancy_board_by_stop_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_occupancy_board_by_stop_day,labels={'stop_id':'Stops','board_count':'Total Boardings'}, x="stop_id", y="board_count",color="direction_desc",hover_data=['stop_name','route_id'],)
        else:
            fig2 = px.box(result_max_occupancy_board_by_stop_day,labels={'stop_id':'Stops','occupancy':'Max Daily Occupancy '}, x="stop_id", y="occupancy",color="direction_desc",hover_data=['stop_name','route_id'],)
        fig2.update_layout(
            autosize=True,
            bargap=0.1,
            bargroupgap=0,
            xaxis_type='category',
            barmode="stack",            
            margin=go.layout.Margin(l=0, r=35, t=0, b=0),
            plot_bgcolor="#31302F",
            font=dict(color="white"),
            paper_bgcolor="#31302F",
            yaxis=dict(                                    
                #range=[0, occupancymax+2],
                showticklabels=True,
                showgrid=False,
                fixedrange=True,
                rangemode="nonnegative",
                zeroline=False,
            ),
            xaxis_tickangle=-45,
            hoverlabel=dict(font=dict(size=12))
        )  
        del result_max_occupancy_board_by_stop_day      
        #return  fig2
    else: #routes
        result_max_occupancy_board_by_route_day=max_occupancy_board_by_route_day [ (route_condition3) & (direction_condition3) & (month_condition3) &  (datecondtition3)  & (timecondition3)  & (weekday_condition3)  ].compute()
        if(len(result_max_occupancy_board_by_route_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_occupancy_board_by_route_day,labels={'route_id':'Routes','occupancy':'Total Boardings'}, x="route_id", y="board_count",color="route_id",)
        else:
            fig2 = px.box(result_max_occupancy_board_by_route_day,labels={'route_id':'Routes','occupancy':'Max Daily Occupancy'}, x="route_id", y="occupancy",color="route_id",)
       
        fig2.update_layout(
            autosize=True,
            bargap=0.1,
            xaxis_type='category',
            bargroupgap=0,
            barmode="stack",
            margin=go.layout.Margin(l=0, r=35, t=0, b=0),
            plot_bgcolor="#31302F",
            font=dict(color="white"),
            paper_bgcolor="#31302F",
            yaxis=dict(                                    
                #range=[0, occupancymax+2],
                showticklabels=True,
                showgrid=False,
                fixedrange=True,
                rangemode="nonnegative",
                zeroline=False,
            ),
            xaxis_tickangle=-45,
            hoverlabel=dict(font=dict(size=12))
        )
        del result_max_occupancy_board_by_route_day        
        
    del max_occupancy_board_by_trip_day
    del max_occupancy_board_by_stop_day
    del max_occupancy_board_by_route_day
    return  fig2


# %%
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080, debug=True, use_reloader=False)  
    #app.server.run(threaded=True)

# %%
 
