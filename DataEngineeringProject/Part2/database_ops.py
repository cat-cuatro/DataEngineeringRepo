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

def build_insert_data(crumb, type):
    if type == 'breadcrumb':
        tstamp = int(crumb['ACT_TIME'])
        latitude = float(crumb['GPS_LATITUDE'])
        longitude = float(crumb['GPS_LONGITUDE'])
        direction = 1#int(crumb['DIRECTION'])
        speed = int(crumb['VELOCITY'])
        trip_id = int(crumb['EVENT_NO_TRIP'])
        vals = (tstamp, latitude, longitude, direction, speed, trip_id)
    elif type == 'trip':
        trip_id = int(crumb['EVENT_NO_TRIP'])
        route_id = int(crumb['EVENT_NO_STOP'])
        vehicle_id = int(crumb['VEHICLE_ID'])
        service_key = 'Weekday'#str(crumb[''])
        direction = 1
        vals = (trip_id, route_id, vehicle_id, service_key, direction)

    return vals


def insert(breadcrumbs):
    trip_inserts = []
    breadcrumb_data_inserts =[]
    conn = establish_connection()
    cur = conn.cursor()
    for crumb in breadcrumbs:
                    # tstamp, lat, long, dir, speed, trip_id
        crumb_data = build_insert_data(crumb, 'breadcrumb')
        cmd = f"INSERT INTO breadcrumb VALUES ({crumb_data});"
        breadcrumb_data_inserts.append(cmd)
        trip_data = build_insert_data(crumb, 'trip')
        cmd = f"INSERT INTO trip VALUES ({trip_data});"
        trip_inserts.append(cmd)
    
    for insert in breadcrumb_data_inserts:
        cur.execute(insert)
    for insert in trip_inserts:
        cur.execute(insert)