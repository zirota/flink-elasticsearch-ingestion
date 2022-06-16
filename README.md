# flink-elasticsearch-ingestion

Flink ingestion job for indexing data from one Elasticsearch cluster to another

## Usage

Run the standalone jar

```
java -jar flink-elasticsearch-ingestion-0.1.0-standalone.jar [args]
```

## Development

You can get Elasticsearch and Kibana setup for development pretty quickly by using the provide `docker-compose.yml` file:

```
docker-compose -f dev/docker-compose.yml up -d
```

