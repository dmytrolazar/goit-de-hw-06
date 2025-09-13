from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, my_name

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)
#admin_client.delete_topics(admin_client.list_topics())

def create_topic(topic_name):
    num_partitions = 2
    replication_factor = 1

    topic_name = f'{my_name}_{topic_name}'

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

create_topic('sensors')

[print(topic) for topic in admin_client.list_topics() if my_name in topic]

admin_client.close()


