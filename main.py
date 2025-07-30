import json
import math
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from json import JSONDecodeError
from pytz import timezone

from eta import HKEta

MIN_SEGMENT_SECONDS = 5
MAX_SEGMENT_SECONDS = 3600

hketa = HKEta()
routes = list(hketa.route_list.items())
file_lock = threading.Lock()

previous_gmb_query = 0


def parse_datetime(datetime_str):
    formats = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]
    for fmt in formats:
        try:
            if datetime_str.endswith('Z'):
                datetime_str = datetime_str[:-1] + '+0000'
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    # If none of the formats match, raise an error indicating the issue
    raise ValueError(f"time data '{datetime_str}' does not match any supported format")


def find_best_match(hketa, route_id, stop_index, prev_target_time):
    etas_next = hketa.getEtas(route_id=route_id, seq=stop_index + 1, language="en")
    if not etas_next:
        return None, None

    best_match_eta_time = None
    smallest_diff = float('inf')

    for eta in etas_next:
        if 'eta' not in eta or eta['eta'] is None:
            continue

        current_eta_time = parse_datetime(eta['eta'])
        if current_eta_time < prev_target_time:
            continue

        segment_seconds = (current_eta_time - prev_target_time).total_seconds()

        if MIN_SEGMENT_SECONDS <= segment_seconds <= MAX_SEGMENT_SECONDS:
            if segment_seconds < smallest_diff:
                smallest_diff = segment_seconds
                best_match_eta_time = current_eta_time

    if best_match_eta_time:
        return best_match_eta_time, smallest_diff

    return None, None


def haversine(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return 6371.0 * c


def current_hour():
    now_time = datetime.now(timezone('Asia/Hong_Kong'))
    return now_time.strftime('%H')


def current_weekday():
    now_time = datetime.now(timezone('Asia/Hong_Kong'))
    now_date = now_time.strftime('%Y%m%d')
    if now_date in hketa.holidays:
        return '0'
    return now_time.strftime('%w')


def read_file(file_path, stop_id1, stop_id2):
    dir_name = os.path.dirname(file_path)
    if not os.path.exists(dir_name):
        return None
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            if stop_id1 in data:
                times = data[stop_id1]
                if stop_id2 in times:
                    return times[stop_id2]
                else:
                    return None
            else:
                return None
    except FileNotFoundError:
        pass
    except JSONDecodeError:
        pass
    except Exception as e:
        print(f"Error while reading {file_path}: {e}")
    return None


def write_file(file_path, stop_id1, stop_id2, diff, distance):
    with file_lock:
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        data = {stop_id1: {stop_id2: diff}}
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                if stop_id1 in data:
                    times = data[stop_id1]
                    if stop_id2 in times:
                        time = times[stop_id2]
                        if time < 0 or (distance > 1.5 and time < min(2.0, diff)):
                            times[stop_id2] = diff
                        else:
                            diff = (time * 9 + diff) / 10
                            times[stop_id2] = diff
                    else:
                        times[stop_id2] = diff
                else:
                    data[stop_id1] = {stop_id2: diff}
        except FileNotFoundError:
            pass
        except JSONDecodeError:
            pass
        except Exception as e:
            print(f"Error while reading {file_path}: {e}")
        with open(file_path, 'w') as file:
            json.dump(data, file)


def has_numbers(input_string):
    return any(char.isdigit() for char in input_string)


def roll_chance(route, info):
    global previous_gmb_query
    route_number = route["route"]
    hour = int(current_hour())
    if not any(char.isdigit() for char in route_number):
        return True
    chance = 1
    if 2 <= hour < 5:
        if not (route_number.startswith("N") or route_number.endswith("S")):
            chance = 0.01
    if "gmb" in route["co"]:
        now = round(datetime.now(timezone('Asia/Hong_Kong')).timestamp())
        if now - previous_gmb_query < 5:
            chance = 0
        else:
            previous_gmb_query = now
    info[0] = chance
    return chance > 0 and (chance >= 1 or random.uniform(0, 1) >= chance)


def run():
    key, route = random.choice(routes)
    info = [1]
    while not roll_chance(route, info):
        key, route = random.choice(routes)
    if "stops" not in route:
        return
    stops = route["stops"]
    if len(stops) == 0:
        return
    co, stop_ids = random.choice(list(stops.items()))
    if len(stop_ids) < 2:
        return
    stop_index = random.randint(0, len(stop_ids) - 2)

    stop_id1 = stop_ids[stop_index]
    stop_id2 = stop_ids[stop_index + 1]

    try:
        initial_etas = hketa.getEtas(route_id=key, seq=stop_index, language="en")
        if not initial_etas or not initial_etas[0].get('eta'):
            return

        anchor_time = parse_datetime(initial_etas[0]['eta'])
        _, diff = find_best_match(hketa, key, stop_index, anchor_time)

        if diff is None:
            return

        pos1 = hketa.stop_list[stop_id1]["location"]
        pos2 = hketa.stop_list[stop_id2]["location"]
        distance = haversine(pos1["lat"], pos1["lng"], pos2["lat"], pos2["lng"])

        if "lightRail" in route["co"]:
            diff = max(120.0, diff)

        prefix = stop_id1[0:2]
        hour = current_hour()
        weekday = current_weekday()
        write_file(f"times/{prefix}.json", stop_id1, stop_id2, diff, distance)
        write_file(f"times_hourly/{weekday}/{hour}/{prefix}.json", stop_id1, stop_id2, diff, distance)

        route_number = route["route"]
        chance = info[0]
        print(f"WD{weekday} H{hour}: {route_number:<4} [{chance:.2f}] {stop_id1:<16} > {stop_id2:<16} {f'{distance:.2f}':>5}km {f'{(diff / 60):.2f}':>5}mins")
    except Exception as e:
        print(f"Error while running for eta from {stop_id1} to {stop_id2} ({co}): {e}")

def run_repeatedly():
    while True:
        try:
            run()
        except KeyboardInterrupt:
            print("Program terminated by user")
            break
        except Exception as e:
            print(f"Error while running: {e}")
            continue


def main():
    num_threads = 8
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(run_repeatedly) for _ in range(num_threads)]

        try:
            for future in as_completed(futures):
                future.result()
        except KeyboardInterrupt:
            print("Program terminated by user")
            for future in futures:
                future.cancel()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Program terminated by user")