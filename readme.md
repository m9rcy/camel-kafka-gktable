
```
command to reset offset an application id
bin/kafka-streams-application-reset.sh \
--application-id filtered-global-table-app \
--input-topics input-events \
--bootstrap-servers localhost:9092 \
--reset-offsets --to-earliest
```

```
-- send tombstone
kafka-console-producer \
--topic order-window-filtered-topic \
--bootstrap-server localhost:9092 \
--property "parse.key=true" \
--property "key.separator=:"
```

```
docker exec -it 5fc41573b808 bash
```

# inside container
```
kafka-configs --alter \
--bootstrap-server localhost:9092 \
--entity-type topics \
--entity-name _order-window-filtered-topic \
--add-config cleanup.policy=compact
```

