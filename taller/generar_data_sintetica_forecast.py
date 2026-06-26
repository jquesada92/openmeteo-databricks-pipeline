import json
from pyspark.sql import Row
import random
from datetime import datetime, timedelta
import hashlib
import uuid
import json
import time
from zoneinfo import ZoneInfo

def generate_synthetic_weather_data_json(folder,num_records=100):
    base_time = datetime.now(ZoneInfo("America/Panama"))
    data = []
    for i in range(num_records):
        record = {
            "latitude": 9.033391952514648,
            "longitude": -79.4896240234375,
            "timestamp": str(int((base_time + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0).timestamp())),
            "query_timestamp": (base_time + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "precipitation_probability": random.randint(0, 100),
            "precipitation": round(random.uniform(0, 50), 2),
            "weather_code": random.choice([0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99]),
            "wind_direction_10m": random.randint(0, 360),
            "wind_gusts_10m": round(random.uniform(0, 30), 2),
            "wind_speed_10m": round(random.uniform(0, 20), 2)
        }
        data.append(record)
    json_payload: str = json.dumps(
    data,
    ensure_ascii=False,
    default=str
    )

    content_hash: str = hashlib.sha256(
    json_payload.encode("utf-8")
    ).hexdigest()[:16]

    epoch_ns: int = time.time_ns()
    unique_id: str = uuid.uuid4().hex[:12]

    file_name: str = f'{epoch_ns}_{content_hash}_{unique_id}.json'
    file_path: str = folder + file_name

    print( file_path )

    # Ensure the directory exists before writing
    dbutils.fs.mkdirs(folder)

    # overwrite=False evita sobrescritura accidental
    dbutils.fs.put(file_path, json_payload, False)


output_path = "/Workspace/Users/jaquesada92@outlook.com/openmeteo-databricks-pipeline/taller/data/hourly/"
generate_synthetic_weather_data_json(output_path,100)