from confluent_kafka.admin import AdminClient, NewTopic
from colorama import Fore, Style

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",  # Kafka bootstrap server address.
    "username": "admin",  # Kafka username for authentication.
    "password": "VawEzo1ikLtrA8Ug8THa",  # Kafka password for authentication.
    "security_protocol": "SASL_PLAINTEXT",  # Security protocol used for communication.
    "sasl_mechanism": "PLAIN",  # SASL mechanism used for authentication.
}

# Setting up the client
admin_client = AdminClient(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],  # Connect to Kafka server.
        "security.protocol": kafka_config["security_protocol"],  # Set the security protocol.
        "sasl.mechanisms": kafka_config["sasl_mechanism"],  # Set the SASL mechanism for authentication.
        "sasl.username": kafka_config["username"],  # Set the SASL username for authentication.
        "sasl.password": kafka_config["password"],  # Set the SASL password for authentication.
    }
)

my_name = "andrew_motko"  # Custom prefix for topic names
athlete_event_results = f"{my_name}_athlete_event_results"  # Topic name for athlete event results.
enriched_athlete_avg = f"{my_name}_enriched_athlete_avg"  # Topic name for enriched athlete averages.
num_partitions = 2  # Number of partitions for each topic.
replication_factor = 1  # Replication factor for each topic.

# Creating topics
topics = [
    NewTopic(athlete_event_results, num_partitions, replication_factor),
    NewTopic(enriched_athlete_avg, num_partitions, replication_factor),
]

# Deleting topics (optional, if they exist)
try:
    delete_futures = admin_client.delete_topics(
        [athlete_event_results, enriched_athlete_avg]  # Specify topics to delete
    )
    for topic, future in delete_futures.items():
        try:
            future.result()  # Block until the topic deletion is complete
            print(Fore.RED + f"Topic '{topic}' deleted successfully." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"Failed to delete topic '{topic}': {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"An error occurred during topic deletion: {e}" + Style.RESET_ALL)

# Creating topics
try:
    create_futures = admin_client.create_topics(topics)  # Create the specified topics
    for topic, future in create_futures.items():
        try:
            future.result()  # Block until the topic creation is complete
            print(Fore.GREEN + f"Topic '{topic}' created successfully." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"Failed to create topic '{topic}': {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"An error occurred during topic creation: {e}" + Style.RESET_ALL)

# Listing existing topics
try:
    metadata = admin_client.list_topics(timeout=10)  # List topics from Kafka server with a 10-second timeout
    for topic in metadata.topics.keys():
        if my_name in topic:  # Check if the topic name contains the custom prefix
            print(Fore.YELLOW + f"Topic '{topic}' already exists." + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"Failed to list topics: {e}" + Style.RESET_ALL)
