from elasticsearch import Elasticsearch # type: ignore

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])

#Aggregation: Average Fare Amount by Payment Type
index_name = 'nyctaxi'

query_body_avg_fare = {
    "size": 0,
    "aggs": {
        "avg_fare_by_payment_type": {
            "terms": {
                "field": "payment_type.keyword"
            },
            "aggs": {
                "average_fare": {
                    "avg": {
                        "script": {
                            "source": """
                                if (doc['fare_amount.keyword'].size() > 0 && !doc['fare_amount.keyword'].empty) {
                                    try {
                                        return Double.parseDouble(doc['fare_amount.keyword'].value);
                                    } catch (Exception e) {
                                        return null;
                                    }
                                } else {
                                    return null;
                                }
                                """
                        }
                    }
                }
            }
        }
    }
}

response = es.search(index=index_name, body=query_body_avg_fare)

print("Average Fare Amount by Payment Type:")
for bucket in response['aggregations']['avg_fare_by_payment_type']['buckets']:
    print(f"Payment Type: {bucket['key']}, Average Fare: {bucket['average_fare']['value']}")



#for dev_tool query
# POST /nyctaxi/_search
# {
#   "size": 0,
#   "aggs": {
#     "avg_fare_by_payment_type": {
#       "terms": {
#         "field": "payment_type.keyword"
#       },
#       "aggs": {
#         "average_fare": {
#           "avg": {
#             "script": {
#               "source": """
#                 if (doc['fare_amount.keyword'].size() > 0 && !doc['fare_amount.keyword'].empty) {
#                   try {
#                     return Double.parseDouble(doc['fare_amount.keyword'].value);
#                   } catch (Exception e) {
#                     return null;
#                   }
#                 } else {
#                   return null;
#                 }
#               """
#             }
#           }
#         }
#       }
#     }
#   }
# }