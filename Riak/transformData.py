import json

input_file = "yelp_academic_dataset_business.json"
output_file = "filtered_data.json"

filtered = []

with open(input_file, 'r', encoding='utf-8') as infile:
    for line in infile:
        entry = json.loads(line)
        if entry["state"] == "CA" and "Restaurants" in str(entry.get("categories", "")):
            filtered.append({
                "id": entry["business_id"],
                "value": {k: v for k, v in entry.items() if k != "business_id"}
            })
        if len(filtered) >= 10000:
            break

with open(output_file, 'w', encoding='utf-8') as outfile:
    json.dump(filtered, outfile, indent=2)
