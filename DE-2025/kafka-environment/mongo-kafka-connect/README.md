### MongoDB connector


#### Check connector plugins:
```
curl -s http://localhost:8083/connector-plugins | jq
```

#### Sink for nycdriver my_trip topic
```
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '
{
  "name": "mongo-sink2",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "topics": "my_trip",
    "connection.uri": "mongodb://mongo:27017",
    "database": "my-trip",
    "collection": "events",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}'
```

#### Create Kafka producer, topic and some messages
```
kafka-console-producer.sh \
  --broker-list course-kafka:9092 \
  --topic test-topic
  
  
{"_id": "user-001", "name": "Lev", "email": "lev@example.com"}
{"_id": "user-002", "name": "Danny", "email": "Danny@example.com"}
{"_id": "user-003", "name": "Yoni", "email": "Yoni@example.com"}  
```

#### Connect to mongodb check db events
```
docker exec -it mongo mongosh
  
use mydb
db.events.find().pretty()
```