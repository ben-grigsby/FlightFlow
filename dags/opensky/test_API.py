import requests
import json
import os

url = "https://opensky-network.org/api/states/all"

response = requests.get(url)

output_path = "api_output.txt"

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
else:
    with open(output_path, "w") as f:
        f.write(f"Request failed with status code: {response.status_code}")

# Check file size
file_size_bytes = os.path.getsize(output_path)
file_size_kb = round(file_size_bytes / 1024, 2)

print(f"File size: {file_size_bytes} bytes ({file_size_kb} KB)")