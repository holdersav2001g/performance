import random
import uuid
from datetime import datetime, timedelta
import asyncio
import httpx
import os
from tqdm import tqdm
import pandas as pd
import time

async def generate_and_send_event(client, api_url, event_num, total_events):
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

    try:
        response = await client.post(f"{api_url}/api/events", json=event_data, timeout=30.0)
        response.raise_for_status()
        return True
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred for event {event_num}/{total_events}: {e}")
        print(f"Response content: {e.response.text}")
    except httpx.RequestError as e:
        print(f"An error occurred while requesting event {event_num}/{total_events}: {e}")
    except Exception as e:
        print(f"Unexpected error occurred for event {event_num}/{total_events}: {e}")
    return False

async def generate_test_data(api_url, num_events=10000, concurrent_requests=50, measure_performance=True):
    performance_data = []
    start_time = time.time()
    batch_start_time = start_time
    
    async with httpx.AsyncClient() as client:
        tasks = []
        pbar = tqdm(total=num_events, desc="Events inserted")
        for i in range(num_events):
            if len(tasks) >= concurrent_requests:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if await task:
                        pbar.update(1)
                tasks = list(pending)
            tasks.append(asyncio.create_task(generate_and_send_event(client, api_url, i+1, num_events)))
            
            if measure_performance and (i + 1) % 1000 == 0:
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                performance_data.append({
                    'Batch': (i + 1) // 1000,
                    'Duration (seconds)': batch_duration,
                    'Messages per second': 1000 / batch_duration
                })
                batch_start_time = batch_end_time

        if tasks:
            done, _ = await asyncio.wait(tasks)
            for task in done:
                if await task:
                    pbar.update(1)
        
        pbar.close()
    
    total_duration = time.time() - start_time
    print(f"Generated {num_events} events in {total_duration:.2f} seconds")
    print(f"Average rate: {num_events / total_duration:.2f} events per second")

    if measure_performance:
        df = pd.DataFrame(performance_data)
        df.to_excel('mongodb_performance.xlsx', index=False)
        print("Performance data has been written to mongodb_performance.xlsx")

async def main():
    api_url = os.getenv('API_URL', 'http://localhost:5000')
    num_events = 10000
    measure_performance = True  # Set this to True to measure performance and write to Excel
    await generate_test_data(api_url, num_events, measure_performance=measure_performance)

if __name__ == "__main__":
    asyncio.run(main())