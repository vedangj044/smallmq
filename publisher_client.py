from quic_client import MQClientProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.client import connect
import asyncio
import time
import json
import sys


async def send_message(client, queue_name):
    payload = {
        "_time": 0,
        "payload": "d6d95e464da6763ca98a91cb4087f0e1c7c7870dd94cd8d651fccafe2124bbc1d6d95e464da6763ca98a91cb4087f0e1c7c7870dd94cd8d651fccafe2124bbc1d6d95e464da6763ca98a91cb4087f0e1c7c7870dd94cd8d651fccafe2124bbc186f7e437faa5a7fce15d1ddcb9ea"
    }

    cnt = 0
    for _ in range(500):
        payload["_time"] = time.time()
        client.enque(queue_name, json.dumps(payload))
        
        cnt += 1
        await asyncio.sleep(0.1)


async def main(parallel_queues: int):
    configuration = QuicConfiguration(
        is_client=True
    )
    configuration.load_verify_locations("pycacert.pem")

    async with connect("10.123.138.182", 5434, configuration=configuration, create_protocol=MQClientProtocol) as client:
        print(f" Client state: {client._connected}")

        coros = []
        for q in range(parallel_queues):
            client.create_queue(f'queue{q}')
        
        await asyncio.sleep(5)
        
        for q in range(parallel_queues):
            coros.append(send_message(client, f'queue{q}'))

        await asyncio.gather(*coros)

        waiter = client._loop.create_future()
        return await asyncio.shield(waiter)
    

if __name__ == "__main__":
    parallel_queue = sys.argv[1]
    asyncio.run(main(parallel_queues=int(parallel_queue)))
