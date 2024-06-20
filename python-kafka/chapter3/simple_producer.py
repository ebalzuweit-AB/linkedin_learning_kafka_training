import time

from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094"})


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


some_data_source = range(1234, 1254)

for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce(
        "kafka.learning.orders",
        key=bytes(data),
        value=f"This is order #{data}",
        callback=delivery_report,
    )

    time.sleep(2)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
