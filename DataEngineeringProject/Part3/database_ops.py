import time
import psycopg2
import argparse
import json
import re
import csv

host='localhost'
port=5432
user='postgres'
pw='1337'

insert_breadcrumb = """INSERT INTO breadcrumb
"""
insert_trip = """INSERT INTO trip
"""
insert_stop_events = """INSERT INTO stopevents
"""

def establish_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="breadcrumb_data",
        user=user,
        password=pw,
    )
    conn.autocommit=True
    return conn

def convert(to_convert, type):
    try:
        if type == 'int':
            to_convert = int(to_convert)
        if type == 'float':
            to_convert = float(to_convert)
    except ValueError:
        to_convert = 0
    return to_convert

def build_insert_data(crumb, i, type):
    #print('CRUMB RECEIVED:', crumb)
    if type == 'breadcrumb':
        tstamp = crumb['OPD_DATE'][i]
        latitude = convert(crumb['GPS_LATITUDE'][i], 'float')
        longitude = convert(crumb['GPS_LONGITUDE'][i], 'float')
        direction = 1#convert(crumb['DIRECTION'], 'int')
        speed = convert(crumb['VELOCITY'][i], 'int')
        trip_id = int(crumb['EVENT_NO_TRIP'][i])
        vals = (tstamp, latitude, longitude, direction, speed, trip_id)
    elif type == 'trip':
        trip_id = int(crumb['EVENT_NO_TRIP'][i])
        route_id = int(crumb['EVENT_NO_STOP'][i])
        vehicle_id = int(crumb['VEHICLE_ID'][i])
        service_key = 'Weekday'#str(crumb[''])
        direction = "Out"
        vals = (trip_id, route_id, vehicle_id, service_key, direction)
    return vals

def build_event_insert(stop_events, i, type):
    if type == 'stop_event':
        vehicle_number = int(stop_events['VEHICLE_NUMBER'])
        leave_time = int(stop_events['LEAVE_TIME'])
        train = int(stop_events['TRAIN'])
        route_number = int(stop_events['ROUTE_NUMBER'])
        direction = int(stop_events['DIRECTION'])
        service_key = str(stop_events['SERVICE_KEY'])
        stop_time = int(stop_events['STOP_TIME'])
        arrive_time = int(stop_events['ARRIVE_TIME'])
        dwell = int(stop_events['DWELL'])
        location_id = int(stop_events['LOCATION_ID'])
        door = int(stop_events['DOOR'])
        lift = int(stop_events['LIFT'])
        ons = int(stop_events['ONS'])
        offs = int(stop_events['OFFS'])
        estimate_load = int(stop_events['ESTIMATE_LOAD'])
        maximum_speed = int(stop_events['MAXIMUM_SPEED'])
        train_mileage = float(stop_events['TRAIN_MILEAGE'])
        pattern_distance = float(stop_events['PATTERN_DISTANCE'])
        location_distance = float(stop_events['LOCATION_DISTANCE'])
        x_coordinate = float(stop_events['X_COORDINATE'])
        y_coordinate = float(stop_events['Y_COORDINATE'])
        data_source = int(stop_events['DATA_SOURCE'])
        trip_id = int(stop_events['STOP_EVENT_ID'])
        vals = (
            vehicle_number,
            leave_time,
            train,
            route_number,
            direction,
            service_key,
            stop_time,
            arrive_time,
            dwell,
            location_id,
            door,
            lift,
            ons,
            offs,
            estimate_load,
            maximum_speed,
            train_mileage,
            pattern_distance,
            location_distance,
            x_coordinate,
            y_coordinate,
            data_source,
            trip_id
        )
    elif type == 'trip':
        trip_id = int(stop_events['STOP_EVENT_ID'])
        route_id = int(stop_events['ROUTE_NUMBER']) #???
        vehicle_id = int(stop_events['VEHICLE_NUMBER'])
        service_key = 'Weekday' #nyi
        direction = "Out" #nyi
        vals = (trip_id, route_id, vehicle_id, service_key, direction)
    return vals

def insert(breadcrumbs, stop_events=None):
    trip_inserts = []
    breadcrumb_data_inserts =[]
    conn = establish_connection()
    trip_ids = []
    print('Generating commands for insertions..')
    for i in range(len(breadcrumbs['ACT_TIME'])):
                    # tstamp, lat, long, dir, speed, trip_id
        crumb_data = build_insert_data(breadcrumbs, i, 'breadcrumb')
        #print(len(crumb_data))
        cmd = f"INSERT INTO breadcrumb (tstamp, latitude, longitude, direction, speed, trip_id) VALUES {crumb_data};"
        breadcrumb_data_inserts.append(cmd)
        if breadcrumbs['EVENT_NO_TRIP'][i] not in trip_ids:
            trip_data = build_insert_data(breadcrumbs, i, 'trip')
            cmd = f"INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES {trip_data};"
            trip_inserts.append(cmd)
            #print('adding..', breadcrumbs['EVENT_NO_TRIP'][i])
            trip_ids.append(breadcrumbs['EVENT_NO_TRIP'][i])
            #print(trip_ids)
    if stop_events:
        for i in range(len(stop_events)):
            event = json.loads(stop_events[i])
            stop_event_data = build_event_insert(event, i, 'stop_event')
            cmd = f"""INSERT INTO stopevent (vehicle_number, leave_time, train, route_number, direction, service_key, stop_time, arrive_time, dwell, location_id, door, lift,
            ons, offs, estimated_load, maximum_speed, train_mileage, pattern_distance, location_distance, x_coordinate, y_coordinate, data_source, schedule_status, trip_id) VALUES {stop_event_data};
            """
            if event['STOP_EVENT_ID'] not in trip_ids:
                trip_data = build_event_insert(event, i, 'trip')
                cmd = f"INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES {trip_data};"
                trip_inserts.append(cmd)
                trip_ids.append(event['STOP_EVENT_ID'])

    print('Beginning breadcrumb table insertions..')
    with conn.cursor() as cur:
        print('Beginning trip table insertions..')
        for ins in trip_inserts:
            cur.execute(ins)
        for ins in breadcrumb_data_inserts:
            cur.execute(ins)

    print('Success!')
