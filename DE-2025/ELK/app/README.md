# NYC Taxi Data Pipeline

This project demonstrates how to build a data pipeline using Apache Kafka and Elasticsearch to process and analyze NYC Taxi data. The pipeline consumes data from a Kafka topic and indexes it into an Elasticsearch index for further querying and analysis.

## Project Structure

- **nyc_consumer.py**: A Python script that listens to a Kafka topic (`my_trip`) and writes the consumed data to an Elasticsearch index (`nyctaxi`).
- **nyc_query.py**: A Python script that runs various queries on the `nyctaxi` index in Elasticsearch to analyze the data.

## Requirements

- **Python 3.7+**
- **Elasticsearch 7.x**
- **Kafka**
- **Kibana** (optional, for visualizing and interacting with the data)
- **Python Libraries**:
  - `kafka-python`
  - `elasticsearch`
  
Install the required Python libraries using pip:

```bash
pip install kafka-python elasticsearch
