import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Get all keys (warning: SLOW for large datasets)
start = time.time()
all_keys = r.keys("business:*")
print(f"Found {len(all_keys)} keys in {time.time() - start:.2f} sec")

# Filter restaurants (client-side)
start = time.time()
restaurants = []
for key in all_keys[:1000]:  # Limit to 1000 keys for demo
    data = json.loads(r.get(key))
    if "Restaurants" in data.get("categories", ""):
        restaurants.append(data)

print(f"Found {len(restaurants)} restaurants in {time.time() - start:.2f} sec")