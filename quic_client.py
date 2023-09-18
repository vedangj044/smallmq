from typing import Optional
from urllib.parse import urlparse
from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol, QuicStreamHandler
from aioquic.h0.connection import H0_ALPN, H0Connection
from aioquic.h3.connection import H3_ALPN, ErrorCode, H3Connection
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import (
    QuicEvent,
    StreamDataReceived
)
import logging
import time


class MQClientProtocol(QuicConnectionProtocol):
    

    def __init__(self, quic: QuicConnection, stream_handler: QuicStreamHandler):
        super().__init__(quic, stream_handler)
        self.queue_stream_map = {}
        self.connected_queue = ""


    def _send_ack(self, stream_id):
        self._quic.send_stream_data(
            stream_id, f'4:ack'.encode()
        )
        self.transmit()


    def create_queue(self, queue_name):
        stream_id = self._quic.get_next_available_stream_id()
        self._quic.send_stream_data(
            stream_id, f'1:{queue_name}'.encode()
        )
        self.transmit()
        self.queue_stream_map[queue_name] = stream_id


    def enque(self, queue_name, message):
        stream_id = self.queue_stream_map[queue_name]
        self._quic.send_stream_data(
            stream_id, f'2:{message}'.encode()
        )
        self.transmit()


    def connect_queue(self, queue_name):
        stream_id = self._quic.get_next_available_stream_id()
        self._quic.send_stream_data(
            stream_id, f'3:{queue_name}'.encode()
        )
        self.transmit()
        self.queue_stream_map[stream_id] = queue_name

        self._send_ack(stream_id)


    def message_received_callback(self, queue_name: str, data: str):
        pass


    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, StreamDataReceived):
            data = event.data.decode()
            if data == 'ACK':
                logging.info('ACK')
            else:
                self.message_received_callback(self.queue_stream_map[event.stream_id], data)
                self._send_ack(event.stream_id)