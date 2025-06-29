import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def main():
    dataset = [
        {"business_id": "1", "name": "Cafe Bar", "category": "Cafe"},
        {"business_id": "2", "name": "Bookstore", "category": "Shop"},
        {"business_id": "3", "name": "Pizza Place", "category": "Restaurant"},
    ]

    start_time = time.time()
    for record in dataset:
        key = f"business:{record['business_id']}"
        r.set(key, json.dumps(record))
    end_time = time.time()
    
    print(f"Inserted {len(dataset)} records in {end_time - start_time:.4f} seconds")

if __name__ == '__main__':
    main()