import time
import multiprocessing
from functools import partial
import ast
import json

# Load your data
with open('filtered_data.json') as f:
    data = json.load(f)


# Define a CPU-intensive task (example: parsing all attributes)
def parse_attributes(entry):
    if 'attributes' in entry['value'] and entry['value']['attributes']:
        attrs = entry['value']['attributes']
        parsed_attrs = {}
        for key, value in attrs.items():
            try:
                if value in ['True', 'False']:
                    parsed_attrs[key] = value == 'True'
                elif value.startswith('{') or value.startswith('['):
                    parsed_attrs[key] = ast.literal_eval(value)
                else:
                    parsed_attrs[key] = value
            except:
                parsed_attrs[key] = value
        return parsed_attrs
    return {}


# Single processor version
def single_processor_processing():
    results = []
    for entry in data:
        results.append(parse_attributes(entry))
    return results


# Multi-processor version
def multi_processor_processing():
    with multiprocessing.Pool() as pool:
        results = pool.map(parse_attributes, data)
    return results


# Benchmark function
def benchmark():
    # Warm-up (optional)
    single_processor_processing()
    multi_processor_processing()

    # Single processor test
    start = time.time()
    single_processor_processing()
    single_time = time.time() - start

    # Multi-processor test
    start = time.time()
    multi_processor_processing()
    multi_time = time.time() - start

    print(f"Single processor time: {single_time:.4f} seconds")
    print(f"Multi-processor time: {multi_time:.4f} seconds")
    print(f"Speedup: {single_time / multi_time:.2f}x")


if __name__ == '__main__':
    benchmark()