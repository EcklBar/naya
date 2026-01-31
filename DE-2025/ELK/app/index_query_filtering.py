from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Define the index name
index_name = 'nyctaxi'

query_body_filter = {
    "query": {
        "bool": {
            "must": [
                {"term": {"passenger_count.keyword": "1"}},
                {"term": {"payment_type.keyword": "2"}}
            ]
        }
    }
}

response = es.search(index=index_name, body=query_body_filter)

print("Filtered Trips:")
for hit in response['hits']['hits']:
    print(hit['_source'])


