import asyncio
from typing import Optional
from aioquic.asyncio import serve, QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import (
    QuicEvent,
    StreamDataReceived
)


class MQProtocol(QuicConnectionProtocol):

    conn_queue_mapping = {}
    stream_queue_mapping = {}
    message_queue = {}


    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, StreamDataReceived):
            content = event.data.decode("utf-8").split(":")
            if len(content) == 1:
                raise Exception("Invalid Packet")

            process_code, data = int(content[0]), ":".join(content[1::])

            if process_code == 1:
                self.conn_queue_mapping[data] = {
                    "publisher_conn": self,
                    "publisher_stream": event.stream_id,
                    "subscriber_conn": None,
                }

                self.stream_queue_mapping[event.stream_id] = {"name": data}

                self.message_queue[data] = []

            if process_code == 2:
                queue_name = self.stream_queue_mapping[event.stream_id]["name"]
                self.message_queue[queue_name].append(data)

                if len(self.message_queue[queue_name]) == 1:
                    subscriber = self.conn_queue_mapping[queue_name]["subscriber_conn"]
                    if subscriber is not None:
                        subscriber._quic.send_stream_data(
                            stream_id=self.conn_queue_mapping[queue_name]["subscriber_stream"],
                            data=self.message_queue[queue_name].pop(0).encode(),
                        )
                        subscriber.transmit()

            if process_code == 3:
                self.conn_queue_mapping[data]["subscriber_conn"] = self
                self.conn_queue_mapping[data]["subscriber_stream"] = event.stream_id

            if process_code == 4:
                queue_name = self.stream_queue_mapping[event.stream_id]["name"]
                if (
                    self.conn_queue_mapping[queue_name]["subscriber_conn"] == self
                    and self.conn_queue_mapping[queue_name]["subscriber_stream"] == event.stream_id
                    and len(self.message_queue[queue_name]) != 0
                ):
                    self._quic.send_stream_data(
                        stream_id=event.stream_id,
                        data=self.message_queue[queue_name].pop(0).encode(),
                    )
                    self.transmit()

            self._quic.send_stream_data(stream_id=event.stream_id, data=b"ACK")
            self.transmit()

            # print([(ii, len(self.message_queue[ii])) for ii in self.message_queue])
            # print(
            #     self.conn_queue_mapping, self.stream_queue_mapping, self.message_queue
            # )


async def main() -> None:
    configuration = QuicConfiguration(is_client=False)

    configuration.load_cert_chain("ssl_cert.pem", "ssl_key.pem")

    await serve(
        host="0.0.0.0",
        port=5434,
        configuration=configuration,
        create_protocol=MQProtocol,
    )

    print('Server listening...')

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
