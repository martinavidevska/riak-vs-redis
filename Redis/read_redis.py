import redis
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def main():
    test_keys = [
        "business:VeFfrEZ4iWaecrQg6Eq4cg",
        "business:UFpCraqzFBAhtZqmxmiWsA"
    ]

    start_time = time.time()
    for key in test_keys:
        value = r.get(key)
        if value:
            print(f"Found key: {key}")
    end_time = time.time()
    
    print(f"Read {len(test_keys)} records in {end_time - start_time:.4f} seconds")

if __name__ == '__main__':
    main()