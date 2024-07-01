# Liquidator bot

Before starting the consumer and producer as well, add multiple topics running:

```
docker-compose exec kafka kafka-topics.sh --create --topic account-liquidations --partitions 10 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
```

to increase parallelism
