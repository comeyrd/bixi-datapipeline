from collections import defaultdict
import requests
from datetime import datetime
import time
import db_bixi_dispo

FIELDS = ['num_bikes_available','num_ebikes_available','num_bikes_disabled','num_docks_available','num_docks_disabled','is_installed','is_renting','is_returning']
MINIMUM_WAIT = 2 #s
BASE_URL = "https://gbfs.velobixi.com/gbfs/gbfs.json"
STATION_STATUS = "station_status"

def retreive_urls():
    urls = fetch_json(BASE_URL)
    return find_url_bixi(urls,STATION_STATUS)

def find_url_bixi(urls,name):
    for item in urls["data"]["en"]["feeds"]:
        if item["name"]==name:
            return item["url"]
        
def fetch_json(url):
    try: 
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching JSON from {url}: {e}")
        return {}
def floor_to_previous_15min(posix_timestamp):
    interval = 15 * 60  # 15mn
    floored_timestamp = (posix_timestamp // interval) * interval
    return floored_timestamp

def add_to_agg(agregate_holder,station_status_json):
    for station in station_status_json["data"]["stations"]:
        for field in FIELDS:
            agregate_holder[station["station_id"]][field] += station[field]

import math
def compute_ceiled_means(aggregate_holder, nb_updates,start_time):
    results = []
    for station_id, field_sums in aggregate_holder.items():
        agg = {
            "station_id": station_id,
            "timestamp": start_time
        }

        for field, total in field_sums.items():
            mean = total / nb_updates
            agg[field] = math.ceil(mean)
        results.append(agg)

    return results


def process_bixi_until_death():
    station_status = fetch_json(station_status_url)
    while(True):
        agregate_holder = defaultdict(lambda: defaultdict(int))
        nb_updates = 0
        start_time = floor_to_previous_15min(station_status["last_updated"])
        max_time = start_time + 15 * 60 # 15mn
        #max_time = station_status["last_updated"] + 60 # 15mn
        add_to_agg(agregate_holder,nb_updates,station_status)
        nb_updates+=1
        towait = (station_status["last_updated"] + (station_status["ttl"])) - datetime.now().timestamp()
        if(towait < 0):
            towait = 0
        towait +=MINIMUM_WAIT
        time.sleep(towait)
        prev_timestamp = station_status["last_updated"]
        while True:
            u_station_status = fetch_json(station_status_url)
            if u_station_status["last_updated"] > max_time:
                station_status = u_station_status
                print("Finishing the 15mn")
                break
            if u_station_status["last_updated"] != prev_timestamp :
                print(".",end="")
                add_to_agg(agregate_holder,nb_updates,station_status) 
                nb_updates+=1
                prev_timestamp = u_station_status["last_updated"] 
            towait = (u_station_status["last_updated"] + (u_station_status["ttl"])) - datetime.now().timestamp()
            towait += MINIMUM_WAIT
            if(towait < 0):
                towait = 0
            time.sleep(towait)
        to_send = compute_ceiled_means(agregate_holder,nb_updates,start_time)
        db_bixi_dispo.insert_ceiled_means(to_send)



station_status_url = retreive_urls()
