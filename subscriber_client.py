from quic_client import MQClientProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.asyncio.client import connect
from aioquic.quic.events import QuicEvent, StreamDataReceived
import asyncio
import json
import sys
import time


class SubscriberClient(MQClientProtocol):
    
    cnt = {}

    def message_received_callback(self, queue_name: str, data: str):
        res = json.loads(data)
        t1 = time.time() - res['_time']

        if queue_name not in self.cnt:
            self.cnt[queue_name] = []
        self.cnt[queue_name].append(t1)

        msg = min([len(self.cnt[i]) for i in self.cnt])
        if msg == 500:
            print("Report: ")
            for ii in self.cnt:
                print(f"{ii}: {sum(self.cnt[ii])/len(self.cnt[ii])}")

            

async def main(parallel_queues: int = 5):
    configuration = QuicConfiguration(
        is_client=True
    )
    configuration.load_verify_locations("pycacert.pem")

    async with connect("0.0.0.0", 5434, configuration=configuration, create_protocol=SubscriberClient) as client:
        for q in range(parallel_queues):
            client.connect_queue(f'queue{q}')

        waiter = client._loop.create_future()
        return await asyncio.shield(waiter)
    


if __name__ == "__main__":
    parallel_queue = sys.argv[1]
    asyncio.run(main(parallel_queues=int(parallel_queue)))
