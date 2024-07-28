curl -X "POST" "http://localhost:8083/connectors" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "jdbc-sink-connector2",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "farm-data",
    "connection.url": "jdbc:postgresql://db:5432/grillos",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "auto.create": "true",
    "insert.mode": "insert",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema_registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema_registry:8081",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
'