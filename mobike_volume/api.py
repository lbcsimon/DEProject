#!/usr/bin/python

from flask import Flask
from flask import request
from flask import jsonify
import pymysql
import datetime
import json
app = Flask(__name__)

con = pymysql.connect(host='mysql', port = 3306, user = 'root', password = 'root', db = 'mobike', charset = 'utf8')
cursor = con.cursor()

def getBikePopularity(weekday, hour, location):
    sql = f"""
    SELECT 
        bike_popularity
    FROM
        start
    WHERE weekday = {weekday} and 
          hour = {hour} and 
          location_name like '%{location}%'
    """
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        return json.dumps({'bike_popularity':row[0], 'location': location, 'hour': hour, 'weekday': weekday})
    except:
        return {"response": 404}


def fromove(weekday, hour, from_):
    sql = f"""
    SELECT distinct location_longitude, location_latitude, bike_popularity
    from end e
    WHERE e.location_name like '%{from_}%'
    
    """
    cursor.execute(sql)
    row = cursor.fetchone()
    long = row[0]
    lat = row[1]
    bike = row[2]
    sql = f"""
        SELECT 
            location_name, bike_popularity
        FROM
            start s
        WHERE s.weekday = {weekday} and s.hour = {hour} and
              {long} - 0.1<= s.location_longitude and
              s.location_longitude <= {long} + 0.1 and 
              s.location_latitude <= {lat} + 0.1 and
              {lat} - 0.1 <=s.location_longitude and
              s.bike_popularity > 10
        LIMIT 10    
    """
    cursor.execute(sql)
    rows = cursor.fetchall()
    return json.dumps({'from_':from_, 'weekday': weekday, 'hour': hour, 'have': bike, 'strategy' : [{'to_':row[0], 'gone':row[1]} for row in rows]}, indent=10)

def tomove(weekday, hour,to_):
    sql = f"""
    SELECT distinct location_longitude, location_latitude, bike_popularity
    from start s
    WHERE s.location_name like '%{to_}%'
    
    """
    cursor.execute(sql)
    row = cursor.fetchone()
    long = row[0]
    lat = row[1]
    bike = row[2]
    sql = f"""
        SELECT 
            location_name, bike_popularity
        FROM
            end e
        WHERE e.weekday = {weekday} and e.hour = {hour} and
              {long} - 0.1<= e.location_longitude and
              e.location_longitude <= {long} + 0.1 and 
              e.location_latitude <= {lat} + 0.1 and
              {lat} - 0.1 <= e.location_longitude and
              e.bike_popularity > 10
        LIMIT 10    
    """

    cursor.execute(sql)
    rows = cursor.fetchall()

    return json.dumps({'to_':to_, 'weekday': weekday, 'hour': hour, 'gone': bike, 'strategy' : [{'from_':row[0], 'have':row[1]} for row in rows]}, indent=10)

def moveStrategy(weekday, hour, from_, to_):
    if to_ == "":
        return fromove(weekday, hour, from_)
    else:
        return tomove(weekday, hour, to_)
        

@app.route('/bikepops', methods=['GET', 'POST'])
def bikepops():
    weekday = request.args.get('weekday', datetime.datetime.today().isoweekday())
    hour = request.args.get('hour', datetime.datetime.today().hour)
    location = request.args.get("location", "人民广场")
    print(f"weekday:{weekday}, hour:{hour}, location:{location}")
    return getBikePopularity(weekday, hour, location)


@app.route('/move', methods=['GET', 'POST'])
def move():
    weekday = request.args.get('weekday', datetime.datetime.today().isoweekday())
    hour = request.args.get('hour', datetime.datetime.today().hour)
    from_ = request.args.get("from_", "")
    to_ = request.args.get("to_", "")
    
    return moveStrategy(weekday, hour, from_, to_)


if __name__=='__main__':
    app.run(host='0.0.0.0', debug=True)
