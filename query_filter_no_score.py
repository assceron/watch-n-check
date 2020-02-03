from elasticsearch import Elasticsearch
import json, requests
index = "new_twitter_index"
# create a client instance of Elasticsearch
elastic_client = Elasticsearch("localhost:9200", timeout=30, retry_on_timeout=True)

search_param = {
    "query": {
            "constant_score" : {
                "filter": {
                    "term": {"text": "climate"}
                }
            }
        }
}
# get another response from the cluster
tweets_data = elastic_client.search(index=index, body=search_param)
print('response:', json.dumps(tweets_data, indent=4))
