import pika
import sys

def main():
    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Queue name
    queue_name = 'event_queue'

    try:
        # Check if the queue exists without modifying it
        channel.queue_declare(queue=queue_name, passive=True)
    except pika.exceptions.ChannelClosedByBroker:
        # If the queue doesn't exist, create it (non-durable for this example)
        print(f"Queue '{queue_name}' doesn't exist. Creating it.")
        channel.queue_declare(queue=queue_name, durable=False)
    
    def callback(ch, method, properties, body):
        print(f" [x] Received {body.decode()}")
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # Stop consuming after the first message
        channel.stop_consuming()

    # Set up consumer
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(' [*] Waiting for a message. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            exit(0)