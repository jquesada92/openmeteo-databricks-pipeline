import json
import math
import numpy as np
from datetime import datetime, timezone
import requests


def get_place_name(latitude, longitude):
    url = "https://nominatim.openstreetmap.org/reverse"

    params = {
        "format": "jsonv2",
        "lat": latitude,
        "lon": longitude,
        "zoom": 16,
        "addressdetails": 1
    }

    headers = {
        "User-Agent": "panama-weather-pipeline"
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()

    address = data.get("address", {})

    return {
        "latitude": latitude,
        "longitude": longitude,
        "display_name": data.get("display_name"),
        "place_name": data.get("name"),
        "city": address.get("city") or address.get("town") or address.get("village"),
        "province": address.get("state"),
        "country": address.get("country"),
    }

def now_dt()->datetime:
    return datetime.now(timezone.utc)

def make_json_serializable(obj):
    """
    Converts Python/NumPy objects into JSON-serializable values.
    Handles ndarray, NumPy numbers, NaN, dictionaries and lists.
    """

    if isinstance(obj, np.ndarray):
        return make_json_serializable(obj.tolist())

    if isinstance(obj, np.integer):
        return int(obj)

    if isinstance(obj, np.floating):
        if np.isnan(obj):
            return None
        return float(obj)

    if isinstance(obj, float):
        if math.isnan(obj):
            return None
        return obj

    if isinstance(obj, dict):
        return {
            key: make_json_serializable(value)
            for key, value in obj.items()
        }

    if isinstance(obj, list):
        return [
            make_json_serializable(item)
            for item in obj
        ]

    if isinstance(obj, tuple):
        return [
            make_json_serializable(item)
            for item in obj
        ]

    return obj