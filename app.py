import dash
import flask
import os
from random import randint
import dash_core_components as dcc
import dash_html_components as html
import dataextract
from dash.dependencies import Input, Output, State
import plotly.express as px
from plotly import graph_objs as go
import datetime as dt
import traveler as tt

server = flask.Flask(__name__)
server.secret_key = os.environ.get('secret_key', str(randint(0, 1000000)))
app = dash.Dash(__name__, server=server, meta_tags=[{"name": "viewport", "content": "width=device-width"}])

mapbox_access_token = "pk.eyJ1Ijoidmlzb3ItdnUiLCJhIjoiY2tkdTZteWt4MHZ1cDJ4cXMwMnkzNjNwdSJ9.-O6AIHBGu4oLy3dQ3Tu2XA"
px.set_mapbox_access_token(mapbox_access_token)

# load data
df = dataextract.decompress_pickle('bus_occupancy_jan_through_jun.pbz2')
print(dt.datetime.now(), "data loaded into df")

# session_id = str(uuid.uuid4())

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
                        dcc.Markdown('''# [Statresp.ai](https://statresp.ai) | Nashville Bus Occupancy'''),
                        html.Label([
                            html.Div("""Select Statistic"""),
                            dcc.Dropdown(
                                id='statistic-selection',
                                className='select-dropdown',
                                multi=False,
                                placeholder='Choose a statistic',
                                options=tt.all_statistic_opts,
                                value='MEAN'),
                            html.Div(id='input-select-statistic-dropdown')  # is this needed?
                        ]),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                html.Div("""Select Start Date """),
                                dcc.DatePickerSingle(
                                    id="date-picker-start",
                                    min_date_allowed=dt.datetime(2020, 1, 1),
                                    max_date_allowed=dt.datetime(2020, 6, 30),
                                    initial_visible_month=dt.datetime(2020, 6, 1),
                                    date=dt.datetime(2020, 1, 1).date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                html.Div("""Select End Date """),
                                dcc.DatePickerSingle(
                                    id="date-picker-end",
                                    min_date_allowed=dt.datetime(2020, 1, 1),
                                    max_date_allowed=dt.datetime(2020, 6, 30),
                                    initial_visible_month=dt.datetime(2020, 6, 1),
                                    date=dt.datetime(2020, 6, 30).date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                html.Div("""Select Different Routes"""),
                                dcc.Dropdown(
                                    multi=True,
                                    searchable=True,
                                    id='route-selection-dropdown'
                                ),
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                html.Div("""Select Bus Direction"""),
                                dcc.Dropdown(
                                    id='bus-direction-dropdown',
                                    multi=True
                                ),
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                html.Div("""Select Time Range (Hour of Day)"""),
                                dcc.RangeSlider(
                                    id='time-range-slider',
                                    count=1,
                                    min=-0,
                                    max=2399,
                                    step=10,
                                    value=[0, 2399],
                                    allowCross=False,
                                    marks={
                                        0: 'Midnight',
                                        600: '6 AM',
                                        1200: 'Noon',
                                        1800: '6 PM',
                                        2399: 'Midnight'
                                    }
                                ),
                                html.P(id='output-container-time-range-slider'),
                            ],
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                html.Div("""Select Trip ID"""),
                                dcc.Dropdown(
                                    id='trip-id-dropdown',
                                    searchable=True,
                                    multi=True,
                                )
                            ],
                        ),
                        html.Div(
                            className="row",
                            children=[
                                html.Div(
                                    className="div-for-dropdown",
                                    style={'text-align': 'center'},
                                    children=[
                                        dcc.Markdown(
                                            'Site designed by [ScopeLab from Vanderbilt University]'
                                            '(https://scopelab.ai/) starting from [the Uber Ride Demo from Plotly]'
                                            '(https://github.com/plotly/dash-sample-apps/tree/master/apps/dash-uber-rides-demo).'),
                                        dcc.Markdown(
                                            'Data source: Nashville Metropolitan Transit Authority.'
                                        )
                                    ]
                                ),
                            ]
                        ),
                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="eight columns div-for-charts bg-grey",
                    children=[
                        dcc.Graph(id="map-graph"),
                        html.Div(
                            children=[
                                html.Div("""Select Any of the Bars on the Histogram to Filter by Trip"""),
                            ],
                        ),
                        dcc.Graph(id="histogram"),
                    ],
                ),
            ],
        )
    ]
)


@app.callback(
    Output('route-selection-dropdown', 'options'),
    [Input('route-selection-dropdown', 'search_value'),
     Input('route-selection-dropdown', 'value')]
)
def update_route_multi_options(search_value, value):
    """
    This updates the route options available for selection based on user's search value
    :param search_value : user's route search value
    :param value : the currently selected route values
    """

    # all routes
    route_list = sorted(list({route for route in df.route_id}))

    # default is to show all routes
    if not search_value:
        return [
            {'label': route, 'value': route}
            for route in route_list
        ]
    # when user starts searching, filter routes
    else:
        return [
            {'label': route, 'value': route}
            for route in route_list
            if ((search_value in route) or (value in route))
        ]


@app.callback(
    Output('bus-direction-dropdown', 'options'),
    [Input('route-selection-dropdown', 'value')])
def update_bus_direction_multi_options(route_values):
    """
    This updates the bus direction options available for selection.
    :param route_values: the currently selected route values
    """
    # all routes
    selected_routes_df = df[['route_id', 'direction_desc']]

    # default is to show all directions
    # if user has selected certain routes, filter to show only those directions
    if route_values is not None:
        selected_routes_df = selected_routes_df.loc[selected_routes_df['route_id'].isin(route_values)]

    direction_list = sorted(list({str(direction) for direction in selected_routes_df.direction_desc}))

    return [
        {'label': direction, 'value': direction}
        for direction in direction_list
    ]


@app.callback(
    dash.dependencies.Output('output-container-time-range-slider', 'children'),
    [dash.dependencies.Input('time-range-slider', 'value')])
def update_time_range_slider_output(raw_time_range):
    """
    Updates text in left panel so user can see exactly what times are selected
    :param raw_time_range: list of raw time values (0 through 2399)
    :return: updated text with start and end times
    """
    # converts raw time to HH:MM format
    if raw_time_range is not None:
        start_time = dt.time(raw_time_range[0] // 100, raw_time_range[0] % 100 * 60 // 100)
        end_time = dt.time(raw_time_range[1] // 100, raw_time_range[1] % 100 * 60 // 100)
    return 'Start Time: ' + str(start_time)[:-3] + '\tEnd Time: ' + str(end_time)[:-3]


@app.callback(
    Output('trip-id-dropdown', 'options'),
    [Input('date-picker-start', 'date'),
     Input('date-picker-end', 'date'),
     Input('route-selection-dropdown', 'value'),
     Input('bus-direction-dropdown', 'value'),
     Input('time-range-slider', 'value'),
     Input('trip-id-dropdown', 'search_value')])
def update_trip_id_multi_options(start_date, end_date, route_list, direction_list, raw_time_range, trip_id_search_val):
    """
    Updates the trip ids available for selection based on other query parameters
    :param start_date: user's selected start_date
    :param end_date: user's selected end_date
    :param route_list: user's selected list of routes
    :param direction_list: user's selected list of directions
    :param raw_time_range: user's selected time range
    :param trip_id_search_val: user's trip_id search value
    :return: updated list of trip ids to select from
    """
    date_condition = (df['date_time'] > start_date) & (df['date_time'] < end_date)

    route_condition = True
    direction_condition = True
    trip_id_condition = True

    if route_list is not None:
        route_condition = (df['route_id'].isin(route_list))
    if direction_list is not None:
        direction_condition = (df['direction_desc'].isin(direction_list))
    if trip_id_search_val is not None:
        df['trip_id'] = df['trip_id'].astype(str)
        trip_id_condition = (df['trip_id'].str.contains(trip_id_search_val))

    # convert to timestamp str (e.g., 1200 --> 12:00)
    start_time = dt.time(raw_time_range[0] // 100, raw_time_range[0] % 100 * 60 // 100).strftime("%H:%M:%S")
    end_time = dt.time(raw_time_range[1] // 100, raw_time_range[1] % 100 * 60 // 100).strftime("%H:%M:%S")
    time_condition = (df['arrival_time'] > start_time) & (df['arrival_time'] < end_time)

    selected_df = df.loc[date_condition & route_condition & direction_condition & time_condition & trip_id_condition]
    trip_id_list = sorted(list({trip_id for trip_id in selected_df.trip_id}))

    # filter by user search
    return [
        {'label': trip_id, 'value': trip_id}
        for trip_id in trip_id_list
    ]


@app.callback(
    Output('map-graph', 'figure'),
    [Input('statistic-selection', 'value'),
     Input('date-picker-start', 'date'),
     Input('date-picker-end', 'date'),
     Input('route-selection-dropdown', 'value'),
     Input('bus-direction-dropdown', 'value'),
     Input('time-range-slider', 'value'),
     Input('trip-id-dropdown', 'value')]
)
def update_map_graph(statistic_name, start_date, end_date, route_list, direction_list, raw_time_range, trip_id_list):
    """
    updates the bus occupancy map based on query parameters
    :param statistic_name: mean, min, max, std, var
    :param start_date: user's selected start_date
    :param end_date: user's selected end_date
    :param route_list: user's selected list of routes
    :param direction_list: user's selected list of directions
    :param raw_time_range: user's selected time range
    :param trip_id_list: user's selected list of trip ids
    :return: updated bus occupancy map
    """
    date_condition = (df['date_time'] > start_date) & (df['date_time'] < end_date)

    route_condition = True
    direction_condition = True
    trip_id_condition = True

    if route_list is not None:
        route_condition = (df['route_id'].isin(route_list))
    if direction_list is not None:
        direction_condition = (df['direction_desc'].isin(direction_list))
    if trip_id_list is not None:
        trip_id_condition = (df['trip_id'].isin(trip_id_list))

    # convert to timestamp str (e.g., 1200 --> 12:00)
    start_time = dt.time(raw_time_range[0] // 100, raw_time_range[0] % 100 * 60 // 100).strftime("%H:%M:%S")
    end_time = dt.time(raw_time_range[1] // 100, raw_time_range[1] % 100 * 60 // 100).strftime("%H:%M:%S")
    time_condition = (df['arrival_time'] > start_time) & (df['arrival_time'] < end_time)

    result = df.loc[date_condition & time_condition & route_condition & direction_condition & trip_id_condition]
    result = result[['stop_name', 'stop_sequence', 'stop_lat', 'stop_lon', 'board_count', 'alight_count', 'occupancy']]

    statistic_func = tt.statistic_fun[statistic_name]
    group_by = result.groupby(['stop_name'])
    stats = statistic_func(group_by).reset_index()

    # round value to 2 dec places
    query_label = 'occupancy_' + statistic_name.lower()
    result = stats.rename(columns={'occupancy': query_label})
    result[query_label] = result[query_label].round(2)

    fig = px.scatter_mapbox(result,
                            lat="stop_lat",
                            lon="stop_lon",
                            color=query_label,
                            size=query_label,
                            range_color=[0, 20],
                            text='stop_name',
                            mapbox_style="light",
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            zoom=12)
    fig.update_layout(
        autosize=True,
        margin=go.layout.Margin(l=0, r=35, t=0, b=0)
    )
    return fig


@app.callback(
    Output('histogram', 'figure'),
    [Input('statistic-selection', 'value'),
     Input('date-picker-start', 'date'),
     Input('date-picker-end', 'date'),
     Input('route-selection-dropdown', 'value'),
     Input('bus-direction-dropdown', 'value'),
     Input('time-range-slider', 'value'),
     Input('trip-id-dropdown', 'value')]
)
def update_bar_chart(statistic_name, start_date, end_date, route_list, direction_list, raw_time_range, trip_id_list):
    """
    updates the trip ids displayed in the bus occupancy bar chart based on query parameters

    :param statistic_name: mean, min, max, std, var
    :param start_date: user's selected start_date
    :param end_date: user's selected end_date
    :param route_list: user's selected list of routes
    :param direction_list: user's selected list of directions
    :param raw_time_range: user's selected time range
    :param trip_id_list: user's selected list of trip ids

    :return: updated bus occupancy bar chart
    """
    date_condition = (df['date_time'] > start_date) & (df['date_time'] < end_date)

    route_condition = True
    direction_condition = True
    trip_id_condition = True

    if route_list is not None:
        route_condition = (df['route_id'].isin(route_list))
    if direction_list is not None:
        direction_condition = (df['direction_desc'].isin(direction_list))
    if trip_id_list is not None:
        trip_id_condition = (df['trip_id'].isin(trip_id_list))

    # convert to timestamp str (e.g., 1200 --> 12:00)
    start_time = dt.time(raw_time_range[0] // 100, raw_time_range[0] % 100 * 60 // 100).strftime("%H:%M:%S")
    end_time = dt.time(raw_time_range[1] // 100, raw_time_range[1] % 100 * 60 // 100).strftime("%H:%M:%S")
    time_condition = (df['arrival_time'] > start_time) & (df['arrival_time'] < end_time)

    result = df.loc[date_condition & time_condition & route_condition & direction_condition & trip_id_condition]
    result = result[['trip_id', 'occupancy']]

    statistic_func = tt.statistic_fun[statistic_name]
    group_by = result.groupby(['trip_id'])
    stats = statistic_func(group_by).reset_index()

    # round value to 2 dec places
    query_label = 'occupancy_' + statistic_name.lower()
    result = stats.rename(columns={'occupancy': query_label})
    result[query_label] = result[query_label].round(2)
    result['trip_id'] = result['trip_id'].astype(str)

    fig = px.bar(result, x='trip_id', y=query_label, text=query_label)
    fig.update_layout(
        xaxis_type='category',
        autosize=True,
        margin=go.layout.Margin(l=0, r=35, t=0, b=0)
    )
    return fig


if __name__ == '__main__':
    app.server.run(threaded=True)
