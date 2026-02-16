from kafka import KafkaConsumer
import json
import clickhouse_connect

consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers="localhost:9092",
    group_id="user-logins-consumer",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = clickhouse_connect.get_client(host='localhost', port=8123, username='user', password='strongpassword')

# Создаем таблицу с колонкой sent_to_kafka
client.command("""
CREATE TABLE IF NOT EXISTS user_logins (
    username String,
    event_type String,
    event_time DateTime,
    sent_to_kafka BOOLEAN
) ENGINE = MergeTree()
ORDER BY event_time
""")

for message in consumer:
    data = message.value

    # Проверяем наличие флага sent_to_kafka в полученных данных
    sent_to_kafka = data.get('sent_to_kafka', False)

    print(f"Received: {data}")
    print(f"Sent to Kafka flag: {sent_to_kafka}")

    # Проверяем, что данные были отправлены через Kafka
    if sent_to_kafka:
        try:
            client.command(
                 f"INSERT INTO user_logins (username, event_type, event_time, sent_to_kafka) VALUES ('{data['user']}', '{data['event']}', toDateTime({data['timestamp']}), {sent_to_kafka}))"
            )
            print("Данные вставлены с sent_to_kafka")
        except Exception as e:
            print(f"Ошибка вставки: {e}")
    else:
        print("Данные не из Кафки!!!")
