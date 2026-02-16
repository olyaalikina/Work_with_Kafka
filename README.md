Данный пайплайн обеспечивает передачу данных о событиях пользователей из PostgreSQL в ClickHouse через Kafka. 
Он состоит из двух основных компонентов:
1. Producer (producer.py) - читает данные из PostgreSQL и отправляет их в Kafka;
2. Consumer (consumer.py) - читает данные из Kafka и сохраняет их в ClickHouse.
Чтобы избежать дублирования используется дополнительное логическое поле в таблице — sent_to_kafka BOOLEAN, которое будет сигнализировать, были ли данные уже отправлены в Kafka.

Пошаговый запуск пайплайна:
1) Запустить ZooKeeper (нужен для Kafka);
2) Запустите Kafka (брокер сообщений);
3) Запустите Consumer (слушает Kafka и пишет в ClickHouse);
4) Запустите Producer (читает из PostgreSQL и отправляет в Kafka);
Сначала Kafka, потом Consumer, потом Producer.
