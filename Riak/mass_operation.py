import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
import statistics


class RiakPerformanceTester:
    def __init__(self, bucket_name="yelp"):
        self.bucket = bucket_name
        self.riak_url = "http://localhost:8098/buckets"
        self.keys = []

    def get_keys(self):
        print("Превземам сите клучеви...")
        start = time.time()
        response = requests.get(f"{self.riak_url}/{self.bucket}/keys?keys=true")
        self.keys = response.json().get("keys", [])
        end = time.time()
        print(f"Пронајдени {len(self.keys)} клучеви во {end - start:.2f} сек.")
        return self.keys

    def test_sequential_read(self, num_reads=1000):
        print(f"\nЗапочнувам секвенцијално читање на {num_reads} записи...")
        test_keys = random.sample(self.keys, min(num_reads, len(self.keys)))

        start = time.time()
        successful_reads = 0
        response_times = []

        for key in test_keys:
            req_start = time.time()
            r = requests.get(f"{self.riak_url}/{self.bucket}/keys/{key}")
            req_end = time.time()

            if r.status_code == 200:
                successful_reads += 1
                response_times.append(req_end - req_start)

        end = time.time()
        total_time = end - start

        print(f"Успешно прочитани {successful_reads} записи во {total_time:.2f} сек.")
        print(f"Просечен одговор: {statistics.mean(response_times) * 1000:.2f}ms")
        print(f"Медијан одговор: {statistics.median(response_times) * 1000:.2f}ms")

        return {
            'total_time': total_time,
            'successful_reads': successful_reads,
            'avg_response_time': statistics.mean(response_times),
            'median_response_time': statistics.median(response_times),
            'throughput': successful_reads / total_time
        }

    def test_parallel_read(self, num_reads=1000, num_threads=4):
        print(f"\nЗапочнувам паралелно читање со {num_threads} threads...")
        test_keys = random.sample(self.keys, min(num_reads, len(self.keys)))

        def read_worker(keys_chunk):
            successful = 0
            response_times = []
            for key in keys_chunk:
                req_start = time.time()
                r = requests.get(f"{self.riak_url}/{self.bucket}/keys/{key}")
                req_end = time.time()

                if r.status_code == 200:
                    successful += 1
                    response_times.append(req_end - req_start)
            return successful, response_times

        chunk_size = len(test_keys) // num_threads
        chunks = [test_keys[i:i + chunk_size] for i in range(0, len(test_keys), chunk_size)]

        start = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            results = list(executor.map(read_worker, chunks))
        end = time.time()

        total_successful = sum(r[0] for r in results)
        all_response_times = []
        for r in results:
            all_response_times.extend(r[1])

        total_time = end - start

        print(f"Успешно прочитани {total_successful} записи во {total_time:.2f} сек.")
        print(f"Просечен одговор: {statistics.mean(all_response_times) * 1000:.2f}ms")

        return {
            'total_time': total_time,
            'successful_reads': total_successful,
            'avg_response_time': statistics.mean(all_response_times),
            'throughput': total_successful / total_time,
            'speedup': self.last_sequential_result['throughput'] / (total_successful / total_time) if hasattr(self,
                                                                                                              'last_sequential_result') else 0
        }

    def test_write_performance(self, num_writes=500):
        print(f"\nЗапочнувам тестирање на пишување...")

        test_data = []
        for i in range(num_writes):
            test_data.append({
                'name': f'Test Business {i}',
                'city': f'Test City {i % 10}',
                'stars': random.uniform(1.0, 5.0),
                'review_count': random.randint(1, 1000),
                'categories': 'Test Category'
            })

        start = time.time()
        successful_writes = 0

        for i, data in enumerate(test_data):
            key = f"test_write_{i}_{random.randint(1000, 9999)}"
            try:
                response = requests.put(
                    f"{self.riak_url}/{self.bucket}/keys/{key}",
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(data)
                )
                if response.status_code in [200, 201, 204]:
                    successful_writes += 1
            except Exception as e:
                print(f"Грешка при пишување: {e}")

        end = time.time()
        total_time = end - start

        print(f"Успешно напишани {successful_writes} записи во {total_time:.2f} сек.")
        print(f"Throughput: {successful_writes / total_time:.2f} записи/сек")

        return {
            'total_time': total_time,
            'successful_writes': successful_writes,
            'throughput': successful_writes / total_time
        }

    def test_update_performance(self, num_updates=200):
        """Тестирање на ажурирање"""
        print(f"\nЗапочнувам тестирање на ажурирање...")
        test_keys = random.sample(self.keys, min(num_updates, len(self.keys)))

        start = time.time()
        successful_updates = 0

        for key in test_keys:
            try:
                # GET операција
                r = requests.get(f"{self.riak_url}/{self.bucket}/keys/{key}")
                if r.status_code == 200:
                    data = r.json()
                    # Ажурирај податоци
                    data["review_count"] = data.get("review_count", 0) + 1
                    data["last_updated"] = time.time()

                    # PUT операција
                    response = requests.put(
                        f"{self.riak_url}/{self.bucket}/keys/{key}",
                        headers={"Content-Type": "application/json"},
                        data=json.dumps(data)
                    )
                    if response.status_code in [200, 201, 204]:
                        successful_updates += 1
            except Exception as e:
                print(f"Грешка при ажурирање: {e}")

        end = time.time()
        total_time = end - start

        print(f"Успешно ажурирани {successful_updates} записи во {total_time:.2f} сек.")
        print(f"Throughput: {successful_updates / total_time:.2f} ажурирања/сек")

        return {
            'total_time': total_time,
            'successful_updates': successful_updates,
            'throughput': successful_updates / total_time
        }


# Користење на тестерот
tester = RiakPerformanceTester()
tester.get_keys()

# Тестирање на различни сценарија
seq_results = tester.test_sequential_read(1000)
tester.last_sequential_result = seq_results

par_results = tester.test_parallel_read(1000, 4)
write_results = tester.test_write_performance(500)
update_results = tester.test_update_performance(200)
