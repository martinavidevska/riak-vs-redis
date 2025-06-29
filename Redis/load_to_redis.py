import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

with open("filtered_data.json") as f:
    data = json.load(f)

start = time.time()
for item in data:
    r.set(f"business:{item['id']}", json.dumps(item["value"]))
end = time.time()

print(f"Inserted {len(data)} items in {end - start:.2f} seconds")