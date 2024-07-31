import random
import uuid
from datetime import datetime, timedelta
import asyncio
import os
from tqdm import tqdm
import pandas as pd
import time
import pika
import json

def generate_event(event_num):
    business_date = datetime.now().strftime('%Y-%m-%d')
    event_types = ['FILE', 'MESSAGE', 'DATABASE', 'PROCESS']
    event_statuses = ['SUCCESS', 'FAILURE', 'IN_PROGRESS']
    batch_or_realtime = ['batch', 'realtime']
    resources = ['ResourceA', 'ResourceB', 'ResourceC', 'ResourceD']

    event_type = random.choice(event_types)
    event_data = {
        'eventId': f'EVT#{uuid.uuid4()}',
        'businessDate': business_date,
        'eventName': f'TestEvent_{random.randint(1, 100)}',
        'eventType': event_type,
        'eventStatus': random.choice(event_statuses),
        'eventTime': (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat(),
        'type': 'event',
        'batchOrRealtime': random.choice(batch_or_realtime),
        'resource': random.choice(resources),
        'timestamp': datetime.now().isoformat(),
        'source': 'TestScript',
        'version': '1.0',
        'description': f'This is a test event {event_num}'
    }

    details = {}
    if event_type == 'DATABASE':
        details.update({
            'databaseName': f'DB_{random.randint(1, 100)}',
            'tableName': f'Table_{random.randint(1, 100)}',
            'operation': random.choice(['INSERT', 'UPDATE', 'DELETE', '', None])
        })
    elif event_type == 'FILE':
        details.update({
            'fileName': f'file_{event_num}.txt',
            'fileSize': random.randint(1000, 1000000),
            'fileLocation': f'/path/to/file_{event_num}.txt',
            'numberOfRows': random.randint(1, 1000)
        })
    elif event_type == 'PROCESS':
        details.update({
            'processName': f'Process_{random.randint(1, 100)}',
            'processId': random.randint(1000, 9999)
        })
    elif event_type == 'MESSAGE':
        details.update({
            'messageId': f'MSG#{uuid.uuid4()}',
            'messageQueue': f'MQ_{random.randint(1, 100)}'
        })

    event_data['details'] = details
    return event_data

def generate_test_data(num_events=250000, measure_performance=True):
    performance_data = []
    start_time = time.time()
    batch_start_time = start_time

    # RabbitMQ connection parameters
    parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue='event_queue')

    pbar = tqdm(total=num_events, desc="Events inserted")
    for i in range(num_events):
        event_data = generate_event(i+1)
        
        # Publish the message
        channel.basic_publish(exchange='',
                              routing_key='event_queue',
                              body=json.dumps(event_data))
        
        pbar.update(1)

        if measure_performance and (i + 1) % 1000 == 0:
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            performance_data.append({
                'Batch': (i + 1) // 1000,
                'Duration (seconds)': batch_duration,
                'Messages per second': 1000 / batch_duration
            })
            batch_start_time = batch_end_time

    pbar.close()
    
    # Close the connection
    connection.close()

    total_duration = time.time() - start_time
    print(f"Generated {num_events} events in {total_duration:.2f} seconds")
    print(f"Average rate: {num_events / total_duration:.2f} events per second")

    if measure_performance:
        df = pd.DataFrame(performance_data)
        df.to_excel('rabbitmq_performance.xlsx', index=False)
        print("Performance data has been written to rabbitmq_performance.xlsx")

if __name__ == "__main__":
    num_events = 250000
    measure_performance = True
    generate_test_data(num_events, measure_performance)