import requests

#Security verification
response = requests.get(
    "https://ipaddress:port/_security/_authenticate",
    auth=("elastic", "password"),
    verify=False
)
print(response.status_code)
print(response.json())

#Index mappings
import requests
import urllib3
urllib3.disable_warnings()

ES_HOST = "https://ipaddress:port"
USERNAME = "elastic"
PASSWORD = "password"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

index_name = "entities"  
res = requests.get(
    f"{ES_HOST}/{index_name}/_mapping",
    headers=HEADERS,
    auth=(USERNAME, PASSWORD),
    verify=False
)

mapping = res.json()
print(mapping)


#Retrieve entire index
import requests
import pandas as pd
import urllib3

urllib3.disable_warnings()

ES_HOST = "https://ipaddress:port"
USERNAME = "elastic"
PASSWORD = "password"
HEADERS = {
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
    "Accept": "application/vnd.elasticsearch+json; compatible-with=8"
}

query = {
    "size": 1000,
    "_source": True,  
    "query": {
        "match_all": {}  #Edit to filter
    }
}
res = requests.post(
    f"{ES_HOST}/entities-indexed01/_search?scroll=2m",
    json=query,
    headers=HEADERS,
    auth=(USERNAME, PASSWORD),
    verify=False
)
data = res.json()
scroll_id = data["_scroll_id"]
hits = data["hits"]["hits"]

all_hits = hits
while True:
    scroll_res = requests.post(
        f"{ES_HOST}/_search/scroll",
        json={"scroll": "2m", "scroll_id": scroll_id},
        headers=HEADERS,
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    scroll_data = scroll_res.json()
    batch = scroll_data["hits"]["hits"]
    if not batch:
        break
    all_hits.extend(batch)

docs = [
    {
        "_id": doc["_id"],
        **doc["_source"]
    }
    for doc in all_hits
]
df = pd.DataFrame(docs)

spark_df = spark.createDataFrame(df)
