import json
import requests
import time

BUCKET = "yelp"
RIAK_URL = "http://localhost:8098/buckets"

with open("filtered_data.json") as f:
    data = json.load(f)
start = time.time()
for item in data:
    key = item["id"]
    value = item["value"]
    response = requests.put(
        f"{RIAK_URL}/{BUCKET}/keys/{key}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(value)
    )
end = time.time()
print(f"Inserted {len(data)} items in {end - start:.2f} seconds.")
