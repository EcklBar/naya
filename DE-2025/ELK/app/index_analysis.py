from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

# Define the index name
index_name = 'nyctaxi'


#Total Trips by Vendor
query_body_vendor_aggregation = {
    "size": 0,  # We don't need individual documents, just the aggregation
    "aggs": {
        "trips_by_vendor": {
            "terms": {
                "field": "vendorid.keyword"  # Use the correct field path
            }
        }
    }
}

response = es.search(index=index_name, body=query_body_vendor_aggregation)

print("Total Trips by Vendor:")
for bucket in response['aggregations']['trips_by_vendor']['buckets']:
    print(f"Vendor ID: {bucket['key']}, Total Trips: {bucket['doc_count']}")


# POST /nyctaxi/_search
# {
#     "size": 0, 
#     "aggs": {
#         "trips_by_vendor": {
#             "terms": {
#                 "field": "vendorid.keyword"  
#             }
#         }
#     }
# }