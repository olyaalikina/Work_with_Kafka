import psycopg2
from kafka import KafkaProducer
import json
import time

# Подключения
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db",
    user="admin",
    password="admin",
    host="localhost",
    port=5432
)
# Получаем данные
with conn.cursor() as cur:
    cur.execute("""
        SELECT username, event_type, extract(epoch FROM event_time)
        FROM user_logins 
        WHERE sent_to_kafka = false
    """)
    rows = cur.fetchall()

print(f"Найдено записей: {len(rows)}")

# Отправляем
for row in rows:
    data = {
        "user": row[0],
        "event": row[1],
        "timestamp": float(row[2]), # преобразуем Decimal → float
        "sent_to_kafka": True
    }

    try:
        producer.send("user_events", value=data)
        print(f"Отправлено: {row[0]} - {row[1]}")

        # Обновляем статус
        with conn.cursor() as up:
            up.execute(
                "UPDATE user_logins SET sent_to_kafka = true WHERE username = %s AND event_type = %s AND event_time = to_timestamp(%s)",
                (row[0], row[1], row[2]))
            conn.commit()

    except Exception as e:
        print(f"Ошибка: {e}")

    time.sleep(0.5)

conn.close()
producer.close()
