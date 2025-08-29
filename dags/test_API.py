import requests
import json

url = "https://opensky-network.org/api/states/all"

response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    with open("api_output.txt", "w") as f:
        json.dump(data, f, indent=2)
else:
    with open("api_output.txt", "w") as f:
        f.write(f"Request failed with status code: {response.status_code}")