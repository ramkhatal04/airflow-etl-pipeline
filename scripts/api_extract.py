import requests
import json
from datetime import datetime


API_URL = "https://jsonplaceholder.typicode.com/users"


def extract_users():
    response = requests.get(API_URL, timeout=30)

    response.raise_for_status()

    users = response.json()

    file_name = "/tmp/raw_users.json"

    with open(file_name, "w") as file:
        json.dump(users, file, indent=4)

    print(f"Extracted {len(users)} records")
    print(f"File created: {file_name}")

    return file_name


if __name__ == "__main__":
    extract_users()