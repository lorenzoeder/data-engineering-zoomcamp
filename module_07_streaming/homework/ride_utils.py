import pandas as pd
from dataclasses import dataclass
import json
from datetime import datetime

@dataclass
class Ride:
    lpep_pickup_datetime: str  # ISO-8601 string, empty when missing
    lpep_dropoff_datetime: str  # ISO-8601 string, empty when missing
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

def ride_from_row(row):
    def to_int(val, default=0):
        return int(val) if not pd.isna(val) else default

    def to_float(val, default=0.0):
        return float(val) if not pd.isna(val) else default

    # timestamps: protect against NaT and return ISO-8601 strings
    if pd.isna(row['lpep_pickup_datetime']):
        pickup_str = ""
    else:
        pickup = row['lpep_pickup_datetime']
        pickup_str = pickup.isoformat()

    if pd.isna(row['lpep_dropoff_datetime']):
        dropoff_str = ""
    else:
        dropoff = row['lpep_dropoff_datetime']
        dropoff_str = dropoff.isoformat()

    return Ride(
        lpep_pickup_datetime=pickup_str,
        lpep_dropoff_datetime=dropoff_str,
        PULocationID=to_int(row['PULocationID']),
        DOLocationID=to_int(row['DOLocationID']),
        passenger_count=to_int(row['passenger_count']),
        trip_distance=to_float(row['trip_distance']),
        tip_amount=to_float(row['tip_amount']),
        total_amount=to_float(row['total_amount']),
    )

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)