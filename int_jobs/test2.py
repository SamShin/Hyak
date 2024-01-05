import requests

spark_master_url = "http://n3263.hyak.local:8080"
applications_url = f"{spark_master_url}/json"

response = requests.get(applications_url)

if response.status_code == 200:
    data = response.json()
    print(data)
