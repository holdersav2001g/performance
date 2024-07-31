import random
import uuid
from datetime import datetime, timedelta
import asyncio
import httpx
import os
from tqdm import tqdm

async def generate_and_send_event(client, api_url, event_num, total_events):
    business_date = datetime.now().strftime('%Y-%m-%d')
    event_type = 'FILE'
    event_data = {
        'eventId': f'EVT#{uuid.uuid4()}',
        'businessDate': business_date,
        'eventName': f'TestEvent_{random.randint(1, 100)}',
        'eventType': event_type,
        'eventStatus': random.choice(['SUCCESS', 'FAILURE', 'IN_PROGRESS']),
        'eventTime': (datetime.now() - timedelta(minutes=random.randint(0, 1440))).isoformat(),
        'type': 'event',
        'batchOrRealtime': random.choice(['batch', 'realtime']),
        'resource': random.choice(['ResourceA', 'ResourceB', 'ResourceC', 'ResourceD']),
        'timestamp': datetime.now().isoformat(),
        'source': 'TestScript',
        'version': '1.0',
        'description': f'This is a test event {event_num}',
        'details': {
            'fileName': f'file_{event_num}.txt',
            'fileSize': random.randint(1000, 1000000),
            'fileLocation': f'/path/to/file_{event_num}.txt',
            'numberOfRows': random.randint(1, 1000)
        }
    }

    try:
        response = await client.post(f"{api_url}/api/events", json=event_data, timeout=30.0)
        response.raise_for_status()  # This will raise an exception for 4xx and 5xx status codes
        return True
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred for event {event_num}/{total_events}: {e}")
        print(f"Response content: {e.response.text}")
    except httpx.RequestError as e:
        print(f"An error occurred while requesting event {event_num}/{total_events}: {e}")
    except Exception as e:
        print(f"Unexpected error occurred for event {event_num}/{total_events}: {e}")
    return False

async def generate_test_data(api_url, num_events=250000, concurrent_requests=50):
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
        
        if tasks:
            done, _ = await asyncio.wait(tasks)
            for task in done:
                if await task:
                    pbar.update(1)
        
        pbar.close()

    print(f"Generated {num_events} events")

async def main():
    api_url = os.getenv('API_URL', 'http://localhost:5000')
    num_events = 250000
    await generate_test_data(api_url, num_events)

if __name__ == "__main__":
    asyncio.run(main())