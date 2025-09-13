from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, my_name
import json
import uuid

       
# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'][0],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    # value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    # key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_3'   # Ідентифікатор групи споживачів
)

topic_name = f'{my_name}_alerts'

# Підписка на тему
consumer.subscribe([topic_name])
print(f"Subscribed to topic '{topic_name}'")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()

