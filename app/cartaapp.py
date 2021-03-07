# %%
import dash
import flask
import dash_table
import os
from random import randint
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import plotly.express as px
#import dataextract
from dash.dependencies import Input, Output
from plotly import graph_objs as go
from plotly.graph_objs import *
from datetime import datetime as dt
from datetime import time as tt
import swifter
import dateparser
import os ,sys #,resource
import dask.dataframe as dd
from fastparquet import ParquetFile
import pickle
import dash_bootstrap_components as dbc
import dash_table
from dash.dependencies import Input, Output, State
from dash_extensions import Download
from dash_extensions.snippets import send_data_frame


#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)



# def write_parquet_file():
# df = pd.read_csv('C:/Users/linds/transit-occupancy-dashboard/app/dataprep/chattanooga/data-files/cartaapc_dashboard.csv' , parse_dates=['date','scheduled_datetime','actual_arrival_datetime','trip_date','scheduled_arrival_time','trip_start_time']) 
# df['actual_arrival_time'] = df['actual_arrival_datetime'].dt.time
# df['hour'] = df['actual_arrival_datetime'].dt.hour
# df['minute'] = df['actual_arrival_datetime'].dt.minute
# df['second'] = df['actual_arrival_datetime'].dt.second
# df['trip_start_time'] = df['trip_start_time'].dt.time
# df['triptime'] = df['trip_start_time']
# df['month']=df['date'].dt.month
# df['year']=df['date'].dt.year
# df['day']=df['date'].dt.day
# df['time_day_seconds'] = (df['trip_start_time'].dt.hour*3600+df['trip_start_time'].dt.minute*60+df['trip_start_time'].dt.second)


#sys.exit(0)
# df.to_parquet('C:/Users/linds/transit-occupancy-dashboard/app/dataprep/chattanooga/data-files/cartaapc_dashboard.parquet')
# write_parquet_file()



#resource.setrlimit(resource.RLIMIT_AS, (1e9, 1e9))  
# Data preparation Code. Uncomment as required

# def return_seconds(trip_time,date):
#     row=dateparser.parse(trip_time)
#     d1=dateparser.parse(date)
#     if row is None : 
#         return None
#     else :
#        trip_start_time=row.time()
#        return d1,trip_start_time,trip_start_time.hour*60*60+trip_start_time.minute*60+trip_start_time.second,trip_start_time.hour,trip_start_time.minute


# #df = dataextract.decompress_pickle('data/chattanooga/chattanooga_bus_occupancy_dashboard_20200828_update-3.pbz2')
# df = df[df['occupancy'] >= 0]
# result=df

# result=df.sort_values(['trip_id','route_id','actual_arrival_datetime','direction_id','stop_sequence','stop_id','occupancy'],ascending=False).drop_duplicates(['trip_id','route_id','actual_arrival_datetime','direction_id','stop_sequence','stop_id'],keep='first').sort_index().reset_index()
# r= result[result.duplicated(['trip_id','route_id','actual_arrival_datetime','direction_id','stop_sequence','stop_id'],keep=False)].sort_values(by=['trip_id','route_id','actual_arrival_datetime','direction_id','stop_sequence','stop_id'])
# print(r.head())
# #dataextract.compress_pickle('data/chattanooga/chattanooga_bus_occupancy_dashboard_20200828_update-3', result)
# #sys.exit(0)
# startdate=df.date.min()
# enddate=df.date.max()
# pd.set_option('display.max_columns', None)
# routes=df.route_id.unique() 
# directions=df.direction_desc.unique()
# df.loc[df['direction_desc'] == 'OUTYBOUND', ['direction_desc']] = 'OUTBOUND'
# directions=df.direction_desc.unique()
# print(enddate,startdate,routes,directions,df.dtypes)
# print(df.head())
# #sys.exit(0) 
 


# #sdf = dd.from_pandas(df, npartitions=10)
# #custom= {'occupancy': 'max', 'board_count': 'sum', 'service_period':'first','triptime': 'first','date':'first','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
# custom= {'occupancy': 'max', 'board_count': 'sum', 'service_period':'first','triptime': 'first','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
# max_chat_occupancy_board_by_trip_day=df.groupby(['trip_id','route_id','direction_desc','month','year','date']).agg(custom).reset_index()
# custom= {'occupancy': 'max', 'board_count': 'sum','service_period':'first','time_day_seconds':'first','trip_start_time':'first','dayofweek':'first'}
# max_chat_occupancy_board_by_stop_day=df.groupby(['route_id','trip_id','stop_id','direction_desc','stop_lat','stop_lon','month','year','date']).agg(custom).reset_index()
# #max_chat_occupancy_board_by_stop_day=df.groupby(['route_id','trip_id','stop_id','direction_desc','stop_lat','stop_lon','month','year','date','dayofweek']).agg(custom).reset_index()
# max_chat_occupancy_board_by_route_day=df.groupby(['route_id','direction_desc','month','year','date']).agg(custom).reset_index()
# max_chat_occupancy_board_by_trip_day.to_parquet('app/data/chattanooga/max_chat_occupancy_board_by_trip_day_20210209.parquet', engine='pyarrow')
# print('1')
# max_chat_occupancy_board_by_stop_day.to_parquet('app/data/chattanooga/max_chat_occupancy_board_by_stop_day_20210116.parquet', engine='pyarrow')
# print('2')
# max_chat_occupancy_board_by_route_day.to_parquet('app/data/chattanooga/max_chat_occupancy_board_by_route_day_20210209.parquet', engine='pyarrow')
# custom= {'occupancy': 'max', 'board_count': 'sum'}
# max_chat_occupancy_board_by_stop=max_chat_occupancy_board_by_stop_day.groupby(['route_id','trip_id','stop_id','direction_desc','month','year','routeTripDirYM']).agg(custom).reset_index()



# sys.exit(0)
#set the server and global parameters
server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
#app = dash.Dash(__name__, server=server,meta_tags=[{"name": "viewport", "content": "width=device-width"}])

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], server=server, meta_tags=[
                {"name": "viewport", "content": "width=device-width"}])

app.title='CARTA Occupancy Dashboard'
latInitial=35.045631
lonInitial=-85.309677
mapbox_access_token = "pk.eyJ1Ijoidmlzb3ItdnUiLCJhIjoiY2tkdTZteWt4MHZ1cDJ4cXMwMnkzNjNwdSJ9.-O6AIHBGu4oLy3dQ3Tu2XA"
px.set_mapbox_access_token(mapbox_access_token)

#load all required dataframes as dask
#sdf = dd.read_parquet('data/chattanooga/occupancy.parquet', engine='fastparquet')

max_chat_occupancy_board_by_trip_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_trip_day_20210209.parquet', engine='fastparquet')
max_chat_occupancy_board_by_stop_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_stop_day_20210116.parquet', engine='fastparquet')
#max_chat_occupancy_board_by_stop_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_stop_day_20210209.parquet', engine='fastparquet')
max_chat_occupancy_board_by_route_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_route_day_20210209.parquet', engine='fastparquet')
summaryTable = dd.read_parquet('dataprep/chattanooga/data-files/stopTable.parquet', engine='fastparquet')
summaryTable = summaryTable.compute().head(5)

#find all routes and directions.
startdate=max_chat_occupancy_board_by_trip_day.date.min().compute() 
enddate=max_chat_occupancy_board_by_trip_day.date.max().compute() 

maxOccupancyRouteDay = max_chat_occupancy_board_by_route_day.occupancy.max().compute()
routes=max_chat_occupancy_board_by_trip_day.route_id.unique().compute() 
directions=max_chat_occupancy_board_by_trip_day.direction_desc.unique().compute()
print(enddate,startdate,routes,directions)
#sys.exit(0)
del max_chat_occupancy_board_by_trip_day
del max_chat_occupancy_board_by_stop_day
del max_chat_occupancy_board_by_route_day

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
                        dcc.Markdown('''# CARTA Occupancy'''),

                        
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
                            #style={'fontColor': 'black'}
                            #style={'backgroundColor':'black'},
                            children=[                                
                                dcc.Markdown('''## Select Bus Routes'''),
                                dcc.Dropdown(
                                    id='bus-routes',
                                    multi=True,
                                    options=[
                                        {'label': '{}'.format(str(i)), 'value': '{}'.format(str(i))} for i in routes
                                    ],
         
                                    #value='NYC'
                                ),
                                                            
                                # dcc.Checklist(
                                #         options=[
                                #              {'label': '{}'.format(str(i)), 'value': '{}'.format(str(i))} for i in routes
                                #         ],                                        
                                #         id='bus-routes' ,
                                #          labelStyle={'display': 'inline-block'} ,
                                #     ),   

                                                      
                            ],
                        ),
                        # Change to side-by-side for mobile layout
                        html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        dcc.Markdown('''## Filter Data By Month'''),
                                        #html.P("""Select from the list to filter data by month.""",style={'text-align': 'left' ,'font-weight':'bold'}),
                                        # Dropdown to select times
                                       
                                       dcc.Dropdown(
                                            id='month-selector',
                                            multi=True,
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
  
                                            #labelStyle={'display': 'inline-block'}
                                            #value='NYC'
                                        )
                                       
                                       # dcc.Checklist(
                                       #      id="month-selector",
                                       #      options=[
                                                
                                       #              {'label': 'Jan', 'value': '1'},
                                       #              {'label': 'Feb', 'value': '2'},
                                       #              {'label': 'Mar', 'value': '3'},
                                       #              {'label': 'Apr', 'value': '4'},
                                       #              {'label': 'May', 'value': '5'},
                                       #              {'label': 'June', 'value': '6'},
                                       #              {'label': 'July', 'value': '7'},
                                       #              {'label': 'Aug', 'value': '8'},
                                       #              {'label': 'Sep', 'value': '9'},
                                       #              {'label': 'Oct', 'value': '10'},
                                       #              {'label': 'Nov', 'value': '11'},
                                       #              {'label': 'Dec', 'value': '12'},
                                                
                                                
                                       #      ],
                                            
                                       #      labelStyle={'display': 'inline-block'}                                         
                                       #  )
                                    ],
                                ),
                    html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        dcc.Markdown('''## Filter Data By Day of Week'''),
                                        #html.P("""Select from the list to filter data by month.""",style={'text-align': 'left' ,'font-weight':'bold'}),
                                        # Dropdown to select times
                                        
                                        dcc.Dropdown(
                                            id='day-selector',
                                            multi=True,
                                            options=[
                                                    {'label': 'Mon', 'value': '0'},
                                                    {'label': 'Tue', 'value': '1'},
                                                    {'label': 'Wed', 'value': '2'},
                                                    {'label': 'Thur', 'value': '3'},
                                                    {'label': 'Fri', 'value': '4'},
                                                    {'label': 'Sat', 'value': '5'},
                                                    {'label': 'Sun', 'value': '6'},
                                            ],
             
                                            #value='NYC'
                                        )
                                        
                                        # dcc.Checklist(
                                        #     id="day-selector",
                                        #     options=[
                                                
                                        #             {'label': 'Mon', 'value': '0'},
                                        #             {'label': 'Tue', 'value': '1'},
                                        #             {'label': 'Wed', 'value': '2'},
                                        #             {'label': 'Thur', 'value': '3'},
                                        #             {'label': 'Fri', 'value': '4'},
                                        #             {'label': 'Sat', 'value': '5'},
                                        #             {'label': 'Sun', 'value': '6'},
                                        #     ],
                                            
                                        #     labelStyle={'display': 'inline-block'}                                         
                                        # )
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
                                        
                                        dcc.Dropdown(
                                            id='direction-selector',
                                            multi=True,
                                            options=[
                                                {'label': '{}'.format(str(i)), 'value': '{}'.format(str(i))} for i in directions
                                                 ],

                                            #value='NYC'
                                        )
                                        
                                        # dcc.Checklist( id='direction-selector',
                                        #         options=[
                                        #         {'label': '{}'.format(str(i)), 'value': '{}'.format(str(i))} for i in directions
                                        #          ],
                                                
                                        #         labelStyle={'display': 'inline-block'} ,                                                     
                                        #     ),                                        
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
                        
                        
                        #Occupancy slider
                        # html.Div(className="div-for-dropdown",
                        #             children=[
                        #             dcc.Markdown('''## Filter Data By Occupancy'''),
                        #             #html.P("""Select Time Range""",style={'text-align': 'left' ,'font-weight':'bold'}),
                        #             dcc.RangeSlider(
                        #                 id='occupancy-slider',
                        #                 min=0,
                        #                 max=maxOccupancyRouteDay,
                        #                 step=1,
                        #                 value=[0, maxOccupancyRouteDay],
                        #                 marks={i: '{}'.format(str(i).zfill(2)) for i in range(0, maxOccupancyRouteDay, 5)},
                        #             ),
                                    
                        #                         ]
                        #             ),
                        
                        # Likelihood slider
                        html.Div(className="div-for-dropdown",
                                    children=[
                                    dcc.Markdown('''## Filter Occupancy Table by Likelihood'''),
                                    #html.P("""Select Time Range""",style={'text-align': 'left' ,'font-weight':'bold'}),
                                    dcc.RangeSlider(
                                        id='occupancy-likelihood-slider',
                                        min=0,
                                        max=10,
                                        step=1,
                                        value=[0, 10],
                                        #marks={i: '{}'.format(str(i).zfill(2)) for i in range(0, 1, 5)},
                                        marks={0:'0',1:'0.20',2:'0.30',3:'0.40',4:'0.50',5:'0.60',6:'0.70',7:'0.75',8:'0.80',9:'0.95',10:'1.00'},
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
                    className="eight columns div-for-charts bg-grey", #original
                    #className="col-12 col-lg-8 p-0 m-0 container-fluid",
                    children=[
                        
                        
                        # Radio buttons
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
                       
                        ],

                        ),
                        
                        
                        # Map graph
                         html.Div(className="container-fluid",hidden=False,children=[
                         dcc.Loading(id="loading-icon1",children=[dcc.Graph(id="map-graph"),],type='default'),]),
                        
                        
                        # Test tab container
                        html.Div(className="p-0 m-0 card bg-dark", children=[
                            #dcc.Markdown("Distribution of Energy consumption per mile"),
                            dbc.Tabs(id='tabs-hist', active_tab="trips", children=[
                                 dbc.Tab(label='Show by Trips',
                                         tab_id='trips', className="bg-dark text-white", children=[dcc.Loading(
                                             id="loading-icon2", children=[dcc.Graph(id="histogram"), ], type='default')]),
                                 dbc.Tab(label='Show by Routes',
                                         tab_id='routes', className="bg-dark text-white", children=[dcc.Loading(
                                             id="loading-icon3", children=[dcc.Graph(id="histogram2"), ], type='default')]),
                                 dbc.Tab(label='Occupancy Table',
                                         tab_id='occuTable', className="bg-dark text-white"
                                         
                                          , children=[
                                              # html.Div([
                                              #       html.Button('Load Table', id='btn-nclicks-1', n_clicks=0)
                                              #       #html.Div(id='container-button-timestamp')
                                              #   ])
                                              html.Div([html.Button("Download entire table", id="btn"), Download(id="download")])
                                              
                                              ,dcc.Loading(
                                              
                                              dash_table.DataTable(id='table3',columns=[{"name": i, "id": i} for i in summaryTable.columns],
                                                      data=summaryTable.to_dict('records'),
                                                      # export_format='csv',
                                                      style_table={
                                                          'overflowY': 'scroll'
                                                          ,'overflowX': 'auto'
                                                          , 'height': '35vh'
                                                          , 'width': '800'
                                                      },
                                                      style_as_list_view=True,
                                                      style_cell={'padding': '5px',
                                                                  'color': 'black'},
                                                      style_header={
                                                          'backgroundColor': 'white',
                                                          'fontWeight': 'bold'
                                                      },
                                                      )
                                              )]
                                          
                                          
                                         ),
                               
                                 ]),
                           
                        ]),
                    ]),
            ],
        ),
         html.Div(className="container-fluid ",children=[dcc.Markdown(""),
         dcc.Markdown('Site designed by [ScopeLab](http://scopelab.ai/) starting from [the Uber Ride Demo from Plotly](https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-uber-rides-demo). Data source: [CARTA](http://www.carta-bus.org). Funding for this work has been provided by the National Science Foundation under awards [CNS-2029950](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2029950) and [CNS-2029952](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2029952). Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.',id='footer'),]),     
    ]
)




def return_seconds(time):
    return time.hour*3600+time.minute*60+time.second




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
    Input("time-slider", "value"),
    Input("configure-statistics","value")]
    #Input("occupancy-slider", "value")]
)
#def update_map(start_date, end_date,  busroutes, months, directions,days,occupancykind,timerange,statisticskind,occuslide):
def update_map(start_date, end_date,  busroutes, months, directions,days,occupancykind,timerange,statisticskind):
    #setup time condition
    #max_chat_occupancy_board_by_trip_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_trip_day.parquet', engine='fastparquet')
    #max_chat_occupancy_board_by_route_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_route_day.parquet', engine='fastparquet')
    max_chat_occupancy_board_by_stop_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_stop_day.parquet', engine='fastparquet')
    #max_chat_occupancy_board_by_stop_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_stop_day_20210209.parquet', engine='fastparquet')
    
    # occumin,occumax=occuslide
    # occupancymin=int(occumin)
    # occupancymax=int(occumax)
    
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
        #weekday_condition= ((max_chat_occupancy_board_by_trip_day['dayofweek']).isin(days))
        weekday_condition2 = ((max_chat_occupancy_board_by_stop_day['dayofweek']).isin(days))
    #timecondition = ((max_chat_occupancy_board_by_trip_day['time_day_seconds']>starttime) & (max_chat_occupancy_board_by_trip_day['time_day_seconds']<endtime))
    timecondition2 = ((max_chat_occupancy_board_by_stop_day['time_day_seconds']>starttime) & (max_chat_occupancy_board_by_stop_day['time_day_seconds']<endtime))
    
    #Occupancy condition
    #occupancycondition = ((max_chat_occupancy_board_by_stop_day['occupancy']>=occupancymin) & (max_chat_occupancy_board_by_stop_day['occupancy']<=occupancymax))
    
    #direction condition
    if directions is None or len(directions)==0:
       # direction_condition=(True)
        direction_condition2=(True)
    else:
       # direction_condition= ((max_chat_occupancy_board_by_trip_day['direction_desc']).isin(directions))    
        direction_condition2=((max_chat_occupancy_board_by_stop_day['direction_desc']).isin(directions)) 

    if months is None or len(months)==0:
        month_condition2=True
    else:
        month_condition2=((max_chat_occupancy_board_by_stop_day['month'].isin(months)))
    

    if busroutes is None or len(busroutes)==0:
        #route_condition=(True)
        route_condition2=(True)
    else:
        #print(busroutes)
        #print (max_chat_occupancy_board_by_trip_day['route_id'].dtype)
       # route_condition  = ((max_chat_occupancy_board_by_trip_day['route_id']).isin(busroutes))
        route_condition2 = ((max_chat_occupancy_board_by_stop_day['route_id']).isin(busroutes))

    #datecondtition=((max_chat_occupancy_board_by_trip_day['date'] >= start_date) & (max_chat_occupancy_board_by_trip_day['date'] <= end_date))
    datecondtition2 = ((max_chat_occupancy_board_by_stop_day['date'] >= start_date) & (max_chat_occupancy_board_by_stop_day['date'] <= end_date))
    if(statisticskind=="board_count"):
        finalstatistics="MeanTotalBoardingsByDayPerRoute"    
        #result_max_chat_occupancy_board_by_stop_day=max_chat_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (occupancycondition) & (month_condition2) &(datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','date'])[statisticskind].sum().reset_index()
        result_max_chat_occupancy_board_by_stop_day=max_chat_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (month_condition2) &(datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','date'])[statisticskind].sum().reset_index()
        result_max_chat_occupancy_board_by_stop_day=result_max_chat_occupancy_board_by_stop_day.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon'])[statisticskind].mean().reset_index().compute()
    else:
        finalstatistics="MeanMaxOccupancyDayPerRoute"
        #result_max_chat_occupancy_board_by_stop_day=max_chat_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (occupancycondition) & (month_condition2) &(datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon'])[statisticskind].mean().reset_index().compute()
        result_max_chat_occupancy_board_by_stop_day=max_chat_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (month_condition2) &(datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon'])[statisticskind].mean().reset_index().compute()
    result_max_chat_occupancy_board_by_stop_day[finalstatistics]=result_max_chat_occupancy_board_by_stop_day[statisticskind]
    fig = px.scatter_mapbox(result_max_chat_occupancy_board_by_stop_day,
                                lat="stop_lat",
                                lon="stop_lon",
                                color=finalstatistics,
                                #animation_frame='date',
                                #animation_group='stop_id',
                                range_color=[0, int(10)],
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
    del result_max_chat_occupancy_board_by_stop_day
    del max_chat_occupancy_board_by_stop_day
    return  fig



# %%


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def q75(x):
    return x.quantile(0.75)



@app.callback(
    #Output('histogram', 'figure'),
    [Output('histogram', 'figure'), 
     Output('histogram2', 'figure'), 
     Output('table3', 'data')
     ],
    
    [Input("date-picker", "date"),
    Input("date-picker-end", "date"),
    #Input("map-graph-radius", "value"),
    Input('bus-routes', 'value'),
    Input("month-selector", "value"),
    Input("direction-selector", "value"),
    Input("day-selector", "value"),
    Input("occupancy-basis","value"),    
    Input("time-slider", "value"),
    #Input("histogram-trip","value"),
    Input("tabs-hist", "active_tab"),
    Input("configure-statistics","value"),
    #Input("occupancy-slider", "value"),
    Input("occupancy-likelihood-slider","value")
    ]

)
def update_histogram(start_date, end_date,  busroutes, months, directions,days,occupancykind,timerange,histogramtrip,statisticskind,pctslide):
    #setup time condition
    
    if histogramtrip=='stops' and (busroutes is None or len (busroutes) >1):
        return {}
    
    
    max_chat_occupancy_board_by_trip_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_trip_day_20210209.parquet', engine='fastparquet')
    #max_chat_occupancy_board_by_stop_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_stop_day_20210209.parquet', engine='fastparquet')
    max_chat_occupancy_board_by_stop_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_stop_day_20210116.parquet', engine='fastparquet')
    max_chat_occupancy_board_by_route_day=dd.read_parquet('data/chattanooga/max_chat_occupancy_board_by_route_day_20210209.parquet', engine='fastparquet')
    pct_table = dd.read_parquet('data/chattanooga/occupancy_pct.parquet', engine='auto')
    dataTable_Stops = dd.read_parquet('data/chattanooga/stopTableFilter.parquet', engine='fastparquet')
    dataTable_Stops2 = dataTable_Stops
    dataTable_Stops = dataTable_Stops[['route_id', 'trip_id', 'stop_id', 'direction_desc', 'month', 'year','occupancy']]
    
    # occumin,occumax=occuslide
    # occupancymin=int(occumin)
    # occupancymax=int(occumax)
    
    pctmins,pctmaxs=pctslide
    pctmin=int(pctmins)
    pctmax=int(pctmaxs)
    
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
        weekday_condition= ((max_chat_occupancy_board_by_trip_day['dayofweek']).isin(days))
        weekday_condition2 = ((max_chat_occupancy_board_by_stop_day['dayofweek']).isin(days))
        weekday_condition3=((max_chat_occupancy_board_by_route_day['dayofweek']).isin(days))
    
    timecondition = ((max_chat_occupancy_board_by_trip_day['time_day_seconds']>starttime) & (max_chat_occupancy_board_by_trip_day['time_day_seconds']<endtime))
    timecondition2 = ((max_chat_occupancy_board_by_stop_day['time_day_seconds']>starttime) & (max_chat_occupancy_board_by_stop_day['time_day_seconds']<endtime))
    timecondition3=((max_chat_occupancy_board_by_route_day['time_day_seconds']>starttime) & (max_chat_occupancy_board_by_route_day['time_day_seconds']<endtime))
    
    #month condition

    if months is None or len(months)==0:
        month_condition=True
        month_condition2=True
        month_condition3=True
        month_condition4=True
    else:
        month_condition = ((max_chat_occupancy_board_by_trip_day['month'].isin(months)))
        month_condition2=((max_chat_occupancy_board_by_stop_day['month'].isin(months)))
        month_condition3=((max_chat_occupancy_board_by_route_day['month'].isin(months)))
        month_condition4=((dataTable_Stops2['month'].isin(months)))
 
    
    #direction condition
    if directions is None or len(directions)==0:
        direction_condition=(True)
        direction_condition2=direction_condition
        direction_condition3=direction_condition
        direction_condition4=direction_condition
    else:
        direction_condition= ((max_chat_occupancy_board_by_trip_day['direction_desc']).isin(directions))    
        direction_condition2=((max_chat_occupancy_board_by_stop_day['direction_desc']).isin(directions)) 
        direction_condition3=((max_chat_occupancy_board_by_route_day['direction_desc']).isin(directions)) 
        direction_condition4=((dataTable_Stops2['direction_desc']).isin(directions)) 

    if busroutes is None or len(busroutes)==0:
        route_condition=(True)
        route_condition2=route_condition
        route_condition3=route_condition
        route_condition4=route_condition
    else:
        route_condition  = ((max_chat_occupancy_board_by_trip_day['route_id']).isin(busroutes))
        route_condition2 = ((max_chat_occupancy_board_by_stop_day['route_id']).isin(busroutes))
        route_condition3 = ((max_chat_occupancy_board_by_route_day['route_id']).isin(busroutes))
        route_condition4 = ((dataTable_Stops2['route_id']).isin(busroutes))


    #Occupancy condition (this was used when the occupancy slider was implemented)
    # occupancycondition = ((max_chat_occupancy_board_by_trip_day['occupancy']>=occupancymin) & (max_chat_occupancy_board_by_trip_day['occupancy']<=occupancymax))
    # occupancycondition2 = ((max_chat_occupancy_board_by_stop_day['occupancy']>=occupancymin) & (max_chat_occupancy_board_by_stop_day['occupancy']<=occupancymax))
    # occupancycondition3 = ((max_chat_occupancy_board_by_route_day['occupancy']>=occupancymin) & (max_chat_occupancy_board_by_route_day['occupancy']<=occupancymax))
    
    datecondtition=((max_chat_occupancy_board_by_trip_day['date'] >= start_date) & (max_chat_occupancy_board_by_trip_day['date'] <= end_date))
    datecondtition2 = ((max_chat_occupancy_board_by_stop_day['date'] >= start_date) & (max_chat_occupancy_board_by_stop_day['date'] <= end_date))
    datecondtition3 = ((max_chat_occupancy_board_by_route_day['date'] >= start_date) & (max_chat_occupancy_board_by_route_day['date'] <= end_date))
    
    table3 = dataTable_Stops.compute().head(5)
    table3 = table3.to_dict('records')
    
    if histogramtrip=='trips':
        result_max_chat_occupancy_board_by_trip_day = max_chat_occupancy_board_by_trip_day [  (route_condition) & (direction_condition) & (datecondtition) & (month_condition) &(timecondition) & (weekday_condition) ].compute().sort_values(['time_day_seconds'])
        table3 = dataTable_Stops.compute().head(5000).to_dict('records')
        
        if(len(result_max_chat_occupancy_board_by_trip_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_chat_occupancy_board_by_trip_day,labels={'trip_start_time':'Trips','board_count':'Total Boardings'}, x="trip_start_time", y="board_count",color="route_id",hover_data=['route_id'])
        else:
            fig2 = px.box(result_max_chat_occupancy_board_by_trip_day,labels={'trip_start_time':'Trips','occupancy':'Max Daily Occupancy'}, x="trip_start_time", y="occupancy",color="route_id",hover_data=['route_id'])
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
        del result_max_chat_occupancy_board_by_trip_day        
        #return  fig2
    elif histogramtrip=='stops':
        
        result_max_chat_occupancy_board_by_stop_day=max_chat_occupancy_board_by_stop_day [ (route_condition2) & (direction_condition2) & (month_condition2) &  (datecondtition2)  & (timecondition2)  & (weekday_condition2)  ].compute()
        table3 = dataTable_Stops.compute().head(5000).to_dict('records')
         
        if(len(result_max_chat_occupancy_board_by_stop_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_chat_occupancy_board_by_stop_day,labels={'stop_id':'Stops','board_count':'Total Boardings'}, x="stop_id", y="board_count",color="direction_desc",hover_data=['stop_name','route_id'],)
        else:
            fig2 = px.box(result_max_chat_occupancy_board_by_stop_day,labels={'stop_id':'Stops','occupancy':'Max Daily Occupancy '}, x="stop_id", y="occupancy",color="direction_desc",hover_data=['stop_name','route_id'],)
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
        del result_max_chat_occupancy_board_by_stop_day      
        #return  fig2
    
    elif histogramtrip=='occuTable':
        
        ## For some reason this filter line is throwing errors. Needs to be resolved in order for table to be filterable again.
        result_max_chat_occupancy_board_by_stop=dataTable_Stops2.compute() # [ (route_condition4) & (direction_condition4) & (month_condition4) ].compute()
        table1 = result_max_chat_occupancy_board_by_stop
 
        # #marks={0:'0',1:'0.20',2:'0.30',3:'0.40',4:'0.50',5:'0.60',6:'0.70',7:'0.75',8:'0.80',9:'0.95',10:'1.00'},
        
        if pctmin==0:
            table2 = (table1[table1['occupancy']>=0])
        elif pctmin==1:
            table2 = (table1[table1['occupancy']>=table1['twenty_pct_occupancy']])
        elif pctmin==2:
            table2 = (table1[table1['occupancy']>=table1['thirty_pct_occupancy']])
        elif pctmin==3:
            table2 = (table1[table1['occupancy']>=table1['forty_pct_occupancy']])
        elif pctmin==4:
            table2 = (table1[table1['occupancy']>=table1['median_occupancy']])
        elif pctmin==5:
            table2 = (table1[table1['occupancy']>=table1['sixty_pct_occupancy']])
        elif pctmin==6:
            table2 = (table1[table1['occupancy']>=table1['seventy_pct_occupancy']])
        elif pctmin==7:
            table2 = (table1[table1['occupancy']>=table1['seventy_fifth_pct_occupancy']])
        elif pctmin==8:
            table2 = (table1[table1['occupancy']>=table1['eighty_pct_occupancy']])
        elif pctmin==9:
            table2 = (table1[table1['occupancy']>=table1['ninety_fifth_pct_occupancy']])
        
        if pctmax==1:
            table22 = (table2[table2['occupancy']<=table2['twenty_pct_occupancy']])
        elif pctmax==2:
            table22 = (table2[table2['occupancy']<=table2['thirty_pct_occupancy']])
        elif pctmax==3:
            table22 = (table2[table2['occupancy']<=table2['forty_pct_occupancy']])
        elif pctmax==4:
            table22 = (table2[table2['occupancy']<=table2['median_occupancy']])
        elif pctmax==5:
            table22 = (table2[table2['occupancy']<=table2['sixty_pct_occupancy']])
        elif pctmax==6:
            table22 = (table2[table2['occupancy']<=table2['seventy_pct_occupancy']])
        elif pctmax==7:
            table22 = (table2[table2['occupancy']<=table2['seventy_fifth_pct_occupancy']])
        elif pctmax==8:
            table22 = (table2[table2['occupancy']<=table2['eighty_pct_occupancy']])
        elif pctmax==9:
            table22 = (table2[table2['occupancy']<=table2['ninety_fifth_pct_occupancy']])
        elif pctmax==10:
            table22 = (table2[table2['occupancy']<=table2['max_occupancy']])
        
        table222 = table22[['route_id','trip_id','stop_id','direction_desc','month','year','occupancy']]
        del table22
        table3 = table222.head(5000).to_dict('records')




        #result_max_chat_occupancy_board_by_route_day=max_chat_occupancy_board_by_route_day [ (route_condition3) & (direction_condition3) & (occupancycondition3) & (month_condition3) &  (datecondtition3)  & (timecondition3)  & (weekday_condition3)  ].compute()
        result_max_chat_occupancy_board_by_route_day=max_chat_occupancy_board_by_route_day [ (route_condition3) & (direction_condition3) & (month_condition3) &  (datecondtition3)  & (timecondition3)  & (weekday_condition3)  ].compute()
        if(len(result_max_chat_occupancy_board_by_route_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_chat_occupancy_board_by_route_day,labels={'route_id':'Routes','occupancy':'Total Boardings'}, x="route_id", y="board_count",color="route_id",)
        else:
            fig2 = px.box(result_max_chat_occupancy_board_by_route_day,labels={'route_id':'Routes','occupancy':'Max Daily Occupancy'}, x="route_id", y="occupancy",color="route_id",)
       
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
        
    else: #routes
        #result_max_chat_occupancy_board_by_route_day=max_chat_occupancy_board_by_route_day [ (route_condition3) & (direction_condition3) & (occupancycondition3) & (month_condition3) &  (datecondtition3)  & (timecondition3)  & (weekday_condition3)  ].compute()
        result_max_chat_occupancy_board_by_route_day=max_chat_occupancy_board_by_route_day [ (route_condition3) & (direction_condition3) & (month_condition3) &  (datecondtition3)  & (timecondition3)  & (weekday_condition3)  ].compute()
        table3 = dataTable_Stops.compute().head(5000).to_dict('records')
        
        
        if(len(result_max_chat_occupancy_board_by_route_day.index) ==0) :return {}
        if statisticskind=="board_count":
            fig2 = px.box(result_max_chat_occupancy_board_by_route_day,labels={'route_id':'Routes','occupancy':'Total Boardings'}, x="route_id", y="board_count",color="route_id",)
        else:
            fig2 = px.box(result_max_chat_occupancy_board_by_route_day,labels={'route_id':'Routes','occupancy':'Max Daily Occupancy'}, x="route_id", y="occupancy",color="route_id",)
       
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
        del result_max_chat_occupancy_board_by_route_day        
        
    del max_chat_occupancy_board_by_trip_day
    del max_chat_occupancy_board_by_stop_day
    del max_chat_occupancy_board_by_route_day
    del dataTable_Stops
    del pct_table
    
    fig3=fig2

    return  fig2, fig3 , table3



# %%

## Button -- Download entire stop table
    
@app.callback(
    Output("download", "data"), 
    
    [Input("date-picker", "date"),
    Input("date-picker-end", "date"),
    Input('bus-routes', 'value'),
    Input("month-selector", "value"),
    Input("direction-selector", "value"),
    Input("day-selector", "value"),
    Input("occupancy-basis","value"),    
    Input("time-slider", "value"),
    Input("tabs-hist", "active_tab"),
    Input("configure-statistics","value"),
    #Input("occupancy-slider", "value"),
    Input("occupancy-likelihood-slider","value"),
    Input("btn", "n_clicks")]
    )
             
def generate_csv(start_date, end_date,  busroutes, months, directions,days,occupancykind,timerange,histogramtrip,statisticskind,pctslide,n_nlicks):
    if(n_nlicks):
        dataTable_Stops2 = dd.read_parquet('data/chattanooga/stopTableFilter.parquet', engine='fastparquet')
        
        # occumin,occumax=occuslide
        # occupancymin=int(occumin)
        # occupancymax=int(occumax)
        
        pctmins,pctmaxs=pctslide
        pctmin=int(pctmins)
        pctmax=int(pctmaxs)
        
        
        #month condition
        if months is None or len(months)==0:
            month_condition4=True
        else:
            month_condition4=((dataTable_Stops2['month'].isin(months)))
     
        
        #direction condition
        if directions is None or len(directions)==0:
            direction_condition4=(True)
        else:
            direction_condition4=((dataTable_Stops2['direction_desc']).isin(directions)) 
    
        if busroutes is None or len(busroutes)==0:
            route_condition4=(True)
        else:
            route_condition4 = ((dataTable_Stops2['route_id']).isin(busroutes))
     
       
        
        ## For some reason this filter line is throwing errors. Needs to be resolved.
        result_max_chat_occupancy_board_by_stop=dataTable_Stops2.compute() # [ (route_condition4) & (direction_condition4) & (month_condition4) ].compute()
        table1 = result_max_chat_occupancy_board_by_stop
        
        if pctmin==0:
            table2 = (table1[table1['occupancy']>=0])
        elif pctmin==1:
            table2 = (table1[table1['occupancy']>=table1['twenty_pct_occupancy']])
        elif pctmin==2:
            table2 = (table1[table1['occupancy']>=table1['thirty_pct_occupancy']])
        elif pctmin==3:
            table2 = (table1[table1['occupancy']>=table1['forty_pct_occupancy']])
        elif pctmin==4:
            table2 = (table1[table1['occupancy']>=table1['median_occupancy']])
        elif pctmin==5:
            table2 = (table1[table1['occupancy']>=table1['sixty_pct_occupancy']])
        elif pctmin==6:
            table2 = (table1[table1['occupancy']>=table1['seventy_pct_occupancy']])
        elif pctmin==7:
            table2 = (table1[table1['occupancy']>=table1['seventy_fifth_pct_occupancy']])
        elif pctmin==8:
            table2 = (table1[table1['occupancy']>=table1['eighty_pct_occupancy']])
        elif pctmin==9:
            table2 = (table1[table1['occupancy']>=table1['ninety_fifth_pct_occupancy']])
            
        if pctmax==1:
            table22 = (table2[table2['occupancy']<=table2['twenty_pct_occupancy']])
        elif pctmax==2:
            table22 = (table2[table2['occupancy']<=table2['thirty_pct_occupancy']])
        elif pctmax==3:
            table22 = (table2[table2['occupancy']<=table2['forty_pct_occupancy']])
        elif pctmax==4:
            table22 = (table2[table2['occupancy']<=table2['median_occupancy']])
        elif pctmax==5:
            table22 = (table2[table2['occupancy']<=table2['sixty_pct_occupancy']])
        elif pctmax==6:
            table22 = (table2[table2['occupancy']<=table2['seventy_pct_occupancy']])
        elif pctmax==7:
            table22 = (table2[table2['occupancy']<=table2['seventy_fifth_pct_occupancy']])
        elif pctmax==8:
            table22 = (table2[table2['occupancy']<=table2['eighty_pct_occupancy']])
        elif pctmax==9:
            table22 = (table2[table2['occupancy']<=table2['ninety_fifth_pct_occupancy']])
        elif pctmax==10:
            table22 = (table2[table2['occupancy']<=table2['max_occupancy']])
            
        table222 = table22[['route_id','trip_id','stop_id','direction_desc','month','year','occupancy']]
        del table22           
        
        return send_data_frame(table222.to_csv, filename="stopTable.csv")










# %%
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080, debug=True, use_reloader=False)  
    #app.server.run(threaded=True)

# %%
 

# df = max_chat_occupancy_board_by_trip_day=dd.read_parquet('C:/users/linds/transit-occupancy-dashboard/app/data/chattanooga/max_chat_occupancy_board_by_trip_day.parquet', engine='fastparquet')
# df.shape[0].compute()

#routedf = max_chat_occupancy_board_by_trip_day=dd.read_parquet('C:/users/linds/transit-occupancy-dashboard/app/data/chattanooga/max_chat_occupancy_board_by_trip_day.parquet', engine='fastparquet')
#df = max_chat_occupancy_board_by_route_day=dd.read_parquet('C:/users/linds/transit-occupancy-dashboard/app/data/chattanooga/max_chat_occupancy_board_by_route_day.parquet', engine='fastparquet')

