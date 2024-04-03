import os
import time
import uuid
import pandas as pd
import urllib.request
import numpy as np
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random

wmo_code_df = None


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')



def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'South-West',
        'vehicleType': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }


def generate_wmo_data():
    global wmo_code_df
    codes_url = 'https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/HTML/WMO-CODE/WMO4677.HTM'
    wmo_html = pd.read_html(codes_url)
    wmo_code_df = pd.concat(
        [wmo_html[1], wmo_html[3], wmo_html[5], wmo_html[7], wmo_html[9], wmo_html[11], wmo_html[12],
         wmo_html[13]])
    wmo_code_df.drop(['Unnamed: 2'], axis=1, inplace=True)
    wmo_code_df.reset_index(drop=True, inplace=True)
    wmo_code_df.columns = ['Code', 'Description']


def get_weather_details(long, lat, start, end):

    start = start.replace(tzinfo=None).isoformat()
    end = end.replace(tzinfo=None).isoformat()

    url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={long}&start_hour={start}&end_hour={end}&hourly=temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m,wind_direction_10m&timezone=America%2FNew_York'
    contents = urllib.request.urlopen(url).read()
    my_json = contents.decode('utf8').replace("'", '"')
    json_data = json.loads(my_json)

    temp = np.average(json_data['hourly']['temperature_2m'])
    precipitation = np.average(json_data['hourly']['precipitation'])
    humid = np.average(json_data['hourly']['relative_humidity_2m'])
    wind_speed = np.average(json_data['hourly']['wind_speed_10m'])
    wind_direction = np.average(json_data['hourly']['wind_direction_10m'])

    lst = json_data['hourly']['weather_code']
    weather_code = max(set(lst), key=lst.count)
    weather_description = wmo_code_df[wmo_code_df['Code']==weather_code]['Description'][weather_code]
    return temp, humid, precipitation, wind_speed, wind_direction, weather_description


def generate_weather_data(device_id, time_start, time_end, location):
    latitude, longitude = location[0], location[1]
    temp, humid, precipitation, wind_speed, wind_direction, weather_description = get_weather_details(longitude, latitude, time_start, time_end)
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': time_end.strftime('%Y-%m-%d %H:%M:%S'),
        'temperature': temp,
        'precipitation': precipitation,
        'windSpeed': wind_speed,
        'windDirection': wind_direction,
        'humidity': humid,
        'weatherType': weather_description,
        'airQualityIndex': random.uniform(0,500)
    }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of Incident'
    }


def generate_vehicle_data(device_id, row):
    location = {'latitude': row['latitude'], 'longitude': row['longitude']}

    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': row['TimeSpan/end'].strftime('%Y-%m-%d %H:%M:%S'),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'South-West',
        'make': 'Audi',
        'model': 'Q3',
        'year': 2024,
        'fuelType': "Petrol"
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key = str(data['id']),
        value = json.dumps(data, default = json_serializer).encode('utf-8'),
        on_delivery = delivery_report
    )
    producer.flush()


def simulate_journey(producer, device_id):
    df = pd.read_csv(os.path.dirname(__file__) + '/../final_df.csv') #pd.read_csv('../final_df.csv')
    df['TimeSpan/begin'] = df['TimeSpan/begin'].apply(lambda x: pd.to_datetime(x))
    df['TimeSpan/end'] = df['TimeSpan/end'].apply(lambda x: pd.to_datetime(x))

    for row in df.iterrows():
        vehicle_data = generate_vehicle_data(device_id, row[1])
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id= 'camera_123')
        weather_data = generate_weather_data(device_id, row[1]['TimeSpan/begin'], row[1]['TimeSpan/end'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)


if __name__ == "__main__":
    generate_wmo_data()

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-CodeWithOm-123')
    except KeyboardInterrupt:
        print('Simulation ended by user')
    except Exception as e:
        print(f'Unexpected Error occurred {e}')



