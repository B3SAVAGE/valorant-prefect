from prefect import task
import requests

@task
def get_endpoint(url):
    response = requests.get(url)
    data = response.json()["data"]
    return data