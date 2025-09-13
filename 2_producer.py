from kafka import KafkaProducer
from configs import kafka_config, my_name
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = f'{my_name}_sensors'
sensor_id = str(uuid.uuid4())

for i in range(30):
    # Відправлення повідомлення в топік
    try:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),  # Часова мітка
            "temperature": round(random.uniform(25, 45),1),
            "humidity": round(random.uniform(15, 85),0)
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully. Data: {data}")

        time.sleep(10)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()
