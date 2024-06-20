from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

serialize = StringSerializer()
p = Producer(
    {
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "acks": "all",
        "compression.type": "gzip",
    }
)


def on_completion(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


data = 1234
p.produce(
    "kafka.learning.orders",
    key=serialize(data.__str__()),
    value=serialize(f"This is order #{data}"),
    callback=on_completion,
)
p.flush()
