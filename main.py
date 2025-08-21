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


def min_diff_cal(first, last, default):
    if first is None and last is None:
        return default
    if first is not None and last is not None:
        return max(default, min(first, last) * 0.75)
    if first is None:
        return max(default, last * 0.75)
    else:
        return max(default, first * 0.75)


def max_diff_cal(first, last, default):
    if first is None and last is None:
        return default
    if first is not None and last is not None:
        return min(default, max(first, last) * 1.25)
    if first is None:
        return min(default, last * 1.25)
    else:
        return min(default, first * 1.25)


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


def find_first_bus_best_match(hketa, route_id, stop_index, prev_target_time):
    etas_next = hketa.getEtas(route_id=route_id, seq=stop_index + 1, language="en")
    if not etas_next or len(etas_next) <= 0 or 'eta' not in etas_next[0] or etas_next[0]['eta'] is None or (len(etas_next) > 1 and 'eta' in etas_next[1] and etas_next[1]['eta'] is not None):
        return None, None

    etas_one_after = hketa.getEtas(route_id=route_id, seq=stop_index + 2, language="en")
    if etas_one_after and len(etas_one_after) > 0 and 'eta' in etas_one_after[0] and etas_one_after[0]['eta'] is not None:
        return None, None

    eta = etas_next[0]
    if 'eta' not in eta or eta['eta'] is None:
        return None, None

    current_eta_time = parse_datetime(eta['eta'])
    if current_eta_time < prev_target_time:
        return None, None

    segment_seconds = (current_eta_time - prev_target_time).total_seconds()

    if MIN_SEGMENT_SECONDS <= segment_seconds <= MAX_SEGMENT_SECONDS:
        return current_eta_time, segment_seconds

    return None, None


def find_last_bus_best_match(hketa, route_id, stop_index, prev_target_time):
    etas_this = hketa.getEtas(route_id=route_id, seq=stop_index, language="en")
    if not etas_this or len(etas_this) <= 0 or 'eta' not in etas_this[0] or etas_this[0]['eta'] is None or (len(etas_this) > 1 and 'eta' in etas_this[1] and etas_this[1]['eta'] is not None):
        return None, None

    etas_next = hketa.getEtas(route_id=route_id, seq=stop_index + 1, language="en")
    if not etas_next:
        return None, None

    etas_previous = hketa.getEtas(route_id=route_id, seq=stop_index - 1, language="en")
    if etas_previous and len(etas_previous) > 0 and 'eta' in etas_previous[0] and etas_previous[0]['eta'] is not None:
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


def find_best_match(hketa, route_id, stop_index, prev_target_time, prefix, stop_id1, stop_id2):
    etas_next = hketa.getEtas(route_id=route_id, seq=stop_index + 1, language="en")
    if not etas_next:
        return None, None

    first_bus_diff = read_file(f"first_bus_times/{prefix}.json", stop_id1, stop_id2)
    last_bus_diff = read_file(f"last_bus_times/{prefix}.json", stop_id1, stop_id2)

    min_diff = min_diff_cal(first_bus_diff, last_bus_diff, MIN_SEGMENT_SECONDS)
    max_diff = max_diff_cal(first_bus_diff, last_bus_diff, MAX_SEGMENT_SECONDS)

    best_match_eta_time = None
    smallest_diff = float('inf')

    for eta in etas_next:
        if 'eta' not in eta or eta['eta'] is None:
            continue

        current_eta_time = parse_datetime(eta['eta'])
        if current_eta_time < prev_target_time:
            continue

        segment_seconds = (current_eta_time - prev_target_time).total_seconds()

        if min_diff <= segment_seconds <= max_diff:
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


def ensure_directory(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

def read_file(file_path, stop_id1, stop_id2):
    with file_lock:
        dir_name = os.path.dirname(file_path)
        ensure_directory(dir_name)
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
        ensure_directory(dir_name)
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

    prefix = stop_id1[0:2]

    try:
        initial_etas = hketa.getEtas(route_id=key, seq=stop_index, language="en")
        if not initial_etas or not initial_etas[0].get('eta'):
            return

        anchor_time = parse_datetime(initial_etas[0]['eta'])

        _, first_bus_diff = find_first_bus_best_match(hketa, key, stop_index, anchor_time)
        if first_bus_diff is not None:
            pos1 = hketa.stop_list[stop_id1]["location"]
            pos2 = hketa.stop_list[stop_id2]["location"]
            distance = haversine(pos1["lat"], pos1["lng"], pos2["lat"], pos2["lng"])

            if "lightRail" in route["co"]:
                first_bus_diff = max(120.0, first_bus_diff)

            hour = current_hour()
            weekday = current_weekday()
            write_file(f"first_bus_times/{prefix}.json", stop_id1, stop_id2, first_bus_diff, distance)

            route_number = route["route"]
            chance = info[0]
            co_display = co.upper()
            if co.casefold() == "lightrail".casefold():
                co_display = "LRT"
            elif co.casefold() == "lrtfeeder".casefold():
                co_display = "MTR-BUS"
            print(f"[F] WD{weekday} H{hour}: {co_display:<7} {route_number:<4} [{chance:.2f}] {stop_id1:<16} > {stop_id2:<16} {f'{distance:.2f}':>5}km {f'{(first_bus_diff / 60):.2f}':>5}mins")

        _, last_bus_diff = find_last_bus_best_match(hketa, key, stop_index, anchor_time)
        if last_bus_diff is not None:
            pos1 = hketa.stop_list[stop_id1]["location"]
            pos2 = hketa.stop_list[stop_id2]["location"]
            distance = haversine(pos1["lat"], pos1["lng"], pos2["lat"], pos2["lng"])

            if "lightRail" in route["co"]:
                last_bus_diff = max(120.0, last_bus_diff)

            hour = current_hour()
            weekday = current_weekday()
            write_file(f"last_bus_times/{prefix}.json", stop_id1, stop_id2, last_bus_diff, distance)

            route_number = route["route"]
            chance = info[0]
            co_display = co.upper()
            if co.casefold() == "lightrail".casefold():
                co_display = "LRT"
            elif co.casefold() == "lrtfeeder".casefold():
                co_display = "MTR-BUS"
            print(f"[L] WD{weekday} H{hour}: {co_display:<7} {route_number:<4} [{chance:.2f}] {stop_id1:<16} > {stop_id2:<16} {f'{distance:.2f}':>5}km {f'{(last_bus_diff / 60):.2f}':>5}mins")


        _, diff = find_best_match(hketa, key, stop_index, anchor_time, prefix, stop_id1, stop_id2)
        if diff is not None:
            pos1 = hketa.stop_list[stop_id1]["location"]
            pos2 = hketa.stop_list[stop_id2]["location"]
            distance = haversine(pos1["lat"], pos1["lng"], pos2["lat"], pos2["lng"])

            if "lightRail" in route["co"]:
                diff = max(120.0, diff)

            hour = current_hour()
            weekday = current_weekday()
            write_file(f"times/{prefix}.json", stop_id1, stop_id2, diff, distance)
            write_file(f"times_hourly/{weekday}/{hour}/{prefix}.json", stop_id1, stop_id2, diff, distance)

            route_number = route["route"]
            chance = info[0]
            co_display = co.upper()
            if co.casefold() == "lightrail".casefold():
                co_display = "LRT"
            elif co.casefold() == "lrtfeeder".casefold():
                co_display = "MTR-BUS"
            print(f"[R] WD{weekday} H{hour}: {co_display:<7} {route_number:<4} [{chance:.2f}] {stop_id1:<16} > {stop_id2:<16} {f'{distance:.2f}':>5}km {f'{(diff / 60):.2f}':>5}mins")
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
    ensure_directory("times")
    ensure_directory("times_hourly")
    ensure_directory("first_bus_times")
    ensure_directory("last_bus_times")
    num_threads = 4
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