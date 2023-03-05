import prefect
from prefect import flow
import call_endpoint
import data_normalize
import call_endpoint
import s3_upload


@flow
def api_to_s3():
    data = call_endpoint.get_endpoint("https://valorant-api.com/v1/agents")
    print(data)
    file = data_normalize.transform(data)
    s3_upload.upload(file)



if __name__ == "__main__":
    api_to_s3()