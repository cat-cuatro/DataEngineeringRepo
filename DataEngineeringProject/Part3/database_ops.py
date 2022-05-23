import time
import psycopg2
import argparse
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

def insert(breadcrumbs):
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

    print('Beginning breadcrumb table insertions..')
    with conn.cursor() as cur:
        print('Beginning trip table insertions..')
        for ins in trip_inserts:
            cur.execute(ins)
        for ins in breadcrumb_data_inserts:
            cur.execute(ins)

    print('Success!')
