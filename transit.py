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
import zipfile

#server = flask.Flask(__name__)
#server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
app = dash.Dash(__name__, server=server,meta_tags=[{"name": "viewport", "content": "width=device-width"}])
app.title='Nashville Occupancy Dashboard'

# %%
mapbox_access_token = "pk.eyJ1Ijoidmlzb3ItdnUiLCJhIjoiY2tkdTZteWt4MHZ1cDJ4cXMwMnkzNjNwdSJ9.-O6AIHBGu4oLy3dQ3Tu2XA"
px.set_mapbox_access_token(mapbox_access_token)

# %%
df = dataextract.decompress_pickle('nashville_bus_occupancy_dashboard-dubey.pbz2')

# %%
routes=df.route_id.unique() 
directions=df.direction_desc.unique() 
# %%


# %%
   #result=find_maximum_by_stop(select_data(busroutes,timemin,timemax,start_date,end_date,directions,days,months,None))

def select_data(routes,timemin,timemax,startdate,enddate,directions,days, months):
    #print(startdate,enddate,timemin,timemax,routes)   
    hourmax=int(timemax)
    if hourmax==24:
        endtime=tt(23,59,59)    
    else:
        minutesmax=int(60*(timemax-hourmax))
        endtime=tt(hourmax,minutesmax,0)
        
    hourmin=int(timemin)
    minutesmin=int(60*(timemin-hourmin))
    starttime=tt(hourmin,minutesmin,0)
    if directions is None or len(directions)==0:
        direction_condition=(True)
    else:
        direction_condition= ((df['direction_desc']).isin(directions))
    if months is None or len(months)==0:
        month_condition=True
    else:
        month_condition = ((df['month'].isin(months)))
    datecondtition=((df['datetime'] >= startdate) & (df['datetime'] <= enddate))    
    time_condition = ((df['timeofday'] >= starttime) & (df['timeofday'] <= endtime))
    if routes is None or len(routes)==0:
        route_condition=(True)
    else:
        route_condition  = ((df['route_id']).isin(routes))
        #print("set route condition")
    if days is None or len(days)==0:
        weekday_condition=(True)
    else:
        weekday_condition= ((df['dayofweek']).isin(days))
    occupancy_condition=(df['occupancy'] >= 0)
    result = df.loc[occupancy_condition &  datecondtition &  route_condition & direction_condition & month_condition & weekday_condition]# & route_condition & datecondtition  & weekday_condition]
    return result
    
    
    
    
    

# %%
def find_maximum_by_stop(dataframe):
    result=dataframe.groupby(['trip_start_time','route_id','direction_desc','year','month','day','stop_id','stop_sequence','stop_name','stop_lat','stop_lon'], as_index=False)['occupancy'].max()
    return result
    

# %%
def find_maximum_by_trip(dataframe):
    result=dataframe.groupby(['trip_start_time','route_id','direction_desc','year','month','day'], as_index=False)['occupancy'].max()
    return result

# %%
#result=find_maximum_by_stop(select_data(None,0,10.5,1,10,None))
#result=find_maximum_by_trip(select_data(None,0,10,1,10,1,30,None))

# %%
#result.head()

# %%

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
                        dcc.Markdown('''# [SmartTransit.ai](https://smarttransit.ai) | Transit Occupancy'''),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                #html.P("""Select Start Date """,style={'text-align': 'left' ,'font-weight':'bold'}),
                                dcc.Markdown('''## Select Start Date'''),
                                dcc.DatePickerSingle(
                                    id="date-picker",
                                    min_date_allowed=dt(2020, 1, 1),
                                    max_date_allowed=dt(2020, 6, 30),
                                    initial_visible_month=dt(2020, 1, 1),
                                    date=dt(2020, 1, 1).date(),
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
                                    min_date_allowed=dt(2020, 1, 1),
                                    max_date_allowed=dt(2020, 6, 30),
                                    initial_visible_month=dt(2020, 6, 30),
                                    date=dt(2020, 6, 30).date(),
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
                                    #dcc.Markdown('''##  Filter by Response Time (min).''') ,
                                    html.Div(style={'text-align': 'center','display':'none'},children=[dcc.Slider(
                                                                id='responsetime-value',
                                                                min=0,
                                                                max=70,
                                                                step=0.5,
                                                                marks={i: '{}'.format(i) for i in range(0, 70,10)},
                                                                value=0
                                                            ),],),    
                                                ]
                                    ),
                                     html.Div(children=[dcc.RadioItems( id='histogram-basis',
                                          options=[                                             
                                              {'label': 'Group By Month', 'value': 'month'},
                                              {'label': 'Group By Day', 'value': 'day'},],
                                        labelStyle={'display': 'inline-block'} ,     
                                        value='month',style={'text-align': 'center'},
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
                        dcc.Graph(id="map-graph"),
                        #html.Div(className="div-for-dropdown", children=[html.P(id='heatmap-text',style={'text-align': 'center'})]),     
                        html.Div(children=[dcc.RadioItems( id='histogram-trip',
                                          options=[                                             
                                              {'label': 'Group By Stops', 'value': 'stops'},
                                              {'label': 'Group By Trips', 'value': 'trips'},],
                                        labelStyle={'display': 'inline-block'} ,     
                                        value='stops',style={'text-align': 'center'},
                                    )]),                  
                        dcc.Graph(id="histogram"),
                    ],
                ),
            ],
        ),
        html.Div(
            className="row",
            children=[html.Div(className="div-for-dropdown", style={'text-align': 'center','display':'none'},
                                            children=[
                                                dcc.Markdown('Site designed by [ScopeLab from Vanderbilt University](http://scopelab.ai/) starting from [the Uber Ride Demo from Plotly](https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-uber-rides-demo). Data source: Nashville Fire Department. Funding for this work has been provided by the National Science Foundation under awards [CNS-1640624](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1640624) and  [IIS-1814958](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1814958). Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.')]),                                   
            ]
        ),
    ]
)



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
    Input("histogram-basis","value"),    
    Input("time-slider", "value"),Input("histogram-trip","value"),]
)
def update_histogram(start_date, end_date,  busroutes, months, directions,days,histogramkind,timerange,histogramtrip):
    timemin,timemax=timerange
    #start_date,end_date
    #bus-routes
    latInitial=36.16228
    lonInitial=-86.774372
    result=select_data(busroutes,timemin,timemax,start_date,end_date,directions,days,months)
    #result=find_maximum_by_stop(result)   'date','month','year','day'
    if histogramtrip == "stops":
        meanresult=result.groupby(['trip_start_time','route_id','stop_id','direction_desc','date','month','year','stop_sequence','stop_name','stop_lat','stop_lon'], as_index=False)['occupancy'].max()
        #print(len(meanresult))    
        if histogramkind=="month":
            meanresult=meanresult.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','month'], as_index=False)['occupancy'].mean()
            animation_frame="month"
            animation_group="stop_id"
        elif histogramkind=="day":
            meanresult=meanresult.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','date'], as_index=False)['occupancy'].mean()    
            animation_frame="date"
            animation_group="stop_id"
        meanresult['occupancy']= meanresult['occupancy'].round().astype(int)
        meanresult['route_id']= meanresult['route_id'].round().astype(str)               
        occupancymax= meanresult['occupancy'].max()
        fig = px.bar(meanresult,labels={'stop_id':'Stops','occupancy':'Mean of Max Occupancy'}, x="stop_id", y="occupancy",color="route_id",animation_frame=animation_frame,animation_group=animation_group,range_y=[0,20])
        
    else:
        meanresult=result.groupby(['trip_start_time','route_id','direction_desc','date','month','year'], as_index=False)['occupancy'].max()
        #print(len(meanresult))    
        if histogramkind=="month":
            meanresult=meanresult.groupby(['trip_start_time','route_id','direction_desc','month'], as_index=False)['occupancy'].mean()
            animation_frame="month"
            animation_group="trip_start_time"
        elif histogramkind=="day":
            meanresult=meanresult.groupby(['trip_start_time','route_id','direction_desc','date'], as_index=False)['occupancy'].mean()    
            animation_frame="date"
            animation_group="trip_start_time"
        meanresult['occupancy']= meanresult['occupancy'].round().astype(int)
        meanresult['route_id']= meanresult['route_id'].round().astype(str)        
        occupancymax= meanresult['occupancy'].max()
      
        fig = px.bar(meanresult, labels={'trip_start_time':'Trips','occupancy':'Mean of Max Occupancy'}, x="trip_start_time", y="occupancy",color="route_id",animation_frame=animation_frame,animation_group=animation_group,range_y=[0,20])
    
    fig.update_layout(
        autosize=True,
        bargap=0.1,
        bargroupgap=0,
        barmode="stack",
        margin=go.layout.Margin(l=0, r=35, t=0, b=0),
        plot_bgcolor="#31302F",
        font=dict(color="white"),
        paper_bgcolor="#31302F",
        yaxis=dict(                                    
            range=[0, 20],
            showticklabels=True,
            showgrid=False,
            fixedrange=True,
            rangemode="nonnegative",
            zeroline=False,
        ),
        xaxis_tickangle=-45,
        hoverlabel=dict(font=dict(size=12))
    )
    return fig

# %%
@app.callback(
    Output('map-graph', 'figure'),
    [Input("date-picker", "date"),
    Input("date-picker-end", "date"),
    #Input("map-graph-radius", "value"),
    Input('bus-routes', 'value'),
    Input("month-selector", "value"),
    Input("direction-selector", "value"),
    Input("day-selector", "value"),
    Input("histogram-basis","value"),    
    Input("time-slider", "value")]
)
def update_map_graph(start_date, end_date,  busroutes, months, directions,days,histogramkind,timerange):
    timemin,timemax=timerange
    #start_date,end_date
    #bus-routes
    latInitial=36.16228
    lonInitial=-86.774372
    result=select_data(busroutes,timemin,timemax,start_date,end_date,directions,days,months)
    #result=find_maximum_by_stop(result)   'date','month','year','day'
    meanresult=result.groupby(['trip_id','route_id','stop_id','direction_desc','date','month','year','stop_sequence','stop_name','stop_lat','stop_lon'], as_index=False)['occupancy'].max()
    #print(len(meanresult))
    # if histogramkind=="month":
    #     meanresult=meanresult.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','month'], as_index=False)['occupancy'].mean()
    #     animation_frame="month"
    #     animation_group="stop_id"
    # elif histogramkind=="day":
    #     meanresult=meanresult.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon','date'], as_index=False)['occupancy'].mean()    
    #     animation_frame="date"
    #     animation_group="stop_id"
    meanresult=meanresult.groupby(['route_id','stop_id','stop_name','stop_lat','stop_lon'], as_index=False)['occupancy'].mean()  
    meanresult['occupancy']= meanresult['occupancy'].round().astype(int)
    occupancymax= meanresult['occupancy'].max()
    #print(len(meanresult))
    fig = px.scatter_mapbox(meanresult,
                            lat="stop_lat",
                            lon="stop_lon",
                            color="occupancy",
                            #animation_frame=animation_frame,
                            #animation_group=animation_group,
                            #size="occupancy",
                            range_color=[0, 20],#color_continuous_midpoint=10,
                            text='stop_name',
                            mapbox_style="light",
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            zoom=10)
    fig.update_layout(
        autosize=True,
        margin=go.layout.Margin(l=0, r=35, t=0, b=0),
        plot_bgcolor="#1E1E1E",
        paper_bgcolor="#1E1E1E",
        hoverlabel=dict(font=dict(size=12))
    )
    
    # fig = go.Figure(go.Densitymapbox(lat=meanresult['stop_lat'], lon=meanresult['stop_lon'],z=meanresult['occupancy'],
    #             customdata=meanresult[['stop_name','route_id','occupancy']],
    #             hovertemplate="%{lat},%{lon} <br> Stop: %{customdata[0]} <br> Route: %{customdata[1]} <br> MeanMax: %{customdata[2]}",
    #             radius=radius),layout=Layout(
    #             autosize=True,
    #             margin=go.layout.Margin(l=0, r=35, t=0, b=0),
    #             showlegend=False,
    #             mapbox=dict(
    #                 accesstoken=mapbox_access_token,
    #                 center=dict(lat=latInitial, lon=lonInitial),  # 40.7272  # -73.991251
    #                 style="light",
    #                 bearing=0,
    #                 zoom=10,
    #             ),
    #             updatemenus=[
    #                 dict(
    #                     buttons=(
    #                         [
    #                             dict(
    #                                 args=[
    #                                     {
    #                                         "mapbox.zoom": 10,
    #                                         "mapbox.center.lon": lonInitial,
    #                                         "mapbox.center.lat": latInitial,
    #                                         "mapbox.bearing": 0,
    #                                         "mapbox.style": "light",
    #                                     }
    #                                 ],
    #                                 label="Reset Zoom",
    #                                 method="relayout",
    #                             )
    #                         ]
    #                     ),
    #                     direction="left",
    #                     pad={"r": 0, "t": 0, "b": 0, "l": 0},
    #                     showactive=False,
    #                     type="buttons",
    #                     x=0.45,
    #                     y=0.02,
    #                     xanchor="left",
    #                     yanchor="bottom",
    #                     bgcolor="#1E1E1E",                    
    #                     borderwidth=1,
    #                     bordercolor="#6d6d6d",
    #                     font=dict(color="#FFFFFF"),
    #                 ),
    #             ],
    #         ),
    #         )
    return fig


# %%


# %%


# %%
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8080, debug=True, use_reloader=False)  
    #app.server.run(threaded=True)

# %%
