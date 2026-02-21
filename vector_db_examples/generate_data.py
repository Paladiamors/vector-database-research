import json
import os
import random

def generate_data(target_count=1000):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, 'data/dataset.json')
    output_path = os.path.join(script_dir, 'data/dataset_large.json')

    with open(data_path, 'r') as f:
        data = json.load(f)

    if not data:
        print("No data found.")
        return

    # Replicate data
    new_data = []
    current_id = 1

    while len(new_data) < target_count:
        for item in data:
            if len(new_data) >= target_count:
                break

            new_item = item.copy()
            new_item["id"] = current_id
            # Optionally add some noise to vector if needed, but for benchmark exact same vectors are fine
            # as long as we search for them.

            new_data.append(new_item)
            current_id += 1

    with open(output_path, 'w') as f:
        json.dump(new_data, f, indent=2)

    print(f"Generated {len(new_data)} items to {output_path}")

if __name__ == "__main__":
    generate_data()
