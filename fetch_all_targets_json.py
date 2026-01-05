import requests
import os
from dotenv import load_dotenv

load_dotenv()
url = "https://goal.base.vn/extapi/v1/target/get"
goal_access_token = os.getenv('GOAL_ACCESS_TOKEN')
payload={'access_token': goal_access_token, 'id': 76276}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.json())
