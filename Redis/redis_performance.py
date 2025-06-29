import redis
import json
import time
import random

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def get_keys():
    print("Fetching all keys...")
    start = time.time()
    keys = r.keys("business:*")
    print(f"Found {len(keys)} keys in {time.time() - start:.2f} sec")
    return keys

def test_read(keys):
    print("\nTesting READ performance...")
    start = time.time()
    for key in keys[:1000]:  # Read first 1000 keys
        r.get(key)
    print(f"Read {len(keys[:1000])} keys in {time.time() - start:.2f} sec")

def test_write(data):
    print("\nTesting WRITE performance...")
    start = time.time()
    for i in range(100):
        r.set(f"test:{random.randint(1,100000)}", json.dumps(data[i % len(data)]))
    print(f"Wrote 100 records in {time.time() - start:.2f} sec")

def test_update(keys):
    print("\nTesting UPDATE performance...")
    start = time.time()
    for key in keys[:100]:
        data = json.loads(r.get(key))
        data["review_count"] = data.get("review_count", 0) + 1
        r.set(key, json.dumps(data))
    print(f"Updated 100 records in {time.time() - start:.2f} sec")

if __name__ == "__main__":
    with open("filtered_data.json") as f:
        sample_data = json.load(f)
    
    keys = get_keys()
    test_read(keys)
    test_write(sample_data)
    test_update(keys)