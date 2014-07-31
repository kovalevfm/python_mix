from __future__ import absolute_import

import logging
from kafka.common import ProduceRequest
from kafka.partitioner import HashedPartitioner
from kafka.protocol import create_message
from collections import defaultdict

log = logging.getLogger("kafka")

BATCH_SEND_DEFAULT_INTERVAL = 20
BATCH_SEND_MSG_COUNT = 20

STOP_ASYNC_PRODUCER = -1


class BatchProducer(object):
    """
    A producer which distributes messages to partitions based on the key
    and send batches

    Params:
    client - The Kafka client instance to use
    partitioner - A partitioner class that will be used to get the partition
        to send the message to. Must be derived from Partitioner
    req_acks - A value indicating the acknowledgements that the server must
               receive before responding to the request
    ack_timeout - Value (in milliseconds) indicating a timeout for waiting
                  for an acknowledgement
    """

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed

    DEFAULT_ACK_TIMEOUT = 2000
    DEFAULT_MESSAGE_LIMIT = 1000000

    def __init__(self, client, partitioner=None,
                 req_acks=ACK_AFTER_CLUSTER_COMMIT,
                 ack_timeout=DEFAULT_ACK_TIMEOUT,
                 message_limit=DEFAULT_MESSAGE_LIMIT):

        self.client = client
        self.req_acks = req_acks
        self.ack_timeout = ack_timeout
        self.message_limit = message_limit
        self.partitioner_class = partitioner or HashedPartitioner
        self.partitioners = {}

    def _next_partition(self, topic, key):
        if topic not in self.partitioners:
            if topic not in self.client.topic_partitions:
                self.client.load_metadata_for_topics(topic)
            self.partitioners[topic] = \
                self.partitioner_class(self.client.topic_partitions[topic])
        partitioner = self.partitioners[topic]
        return partitioner.partition(key, self.client.topic_partitions[topic])

    def send_messages(self, topic, key_msg_list):
        """
        Helper method to send produce requests
        """
        msg_size = 0
        msgset = defaultdict(list)
        resps = []
        try:
            for key, msg in key_msg_list:
                msg_str = create_message(msg)
                if msg_size + len(msg_str) > self.message_limit:
                    reqs = list(ProduceRequest(topic, key, msgs) for key, msgs in msgset.iteritems())
                    resps.extend(self.client.send_produce_request(reqs, acks=self.req_acks,
                                                                  timeout=self.ack_timeout))
                    msgset = defaultdict(list)
                    msg_size = 0
                msgset[self._next_partition(topic, key)].append(msg_str)
                msg_size = msg_size + len(msg_str)
            else:
                reqs = list(ProduceRequest(topic, key, msgs) for key, msgs in msgset.iteritems())
                resps.extend(self.client.send_produce_request(reqs, acks=self.req_acks,
                                                              timeout=self.ack_timeout))
        except Exception:
            log.exception("Unable to send messages")
            raise
        if not all(resp.error == 0 for resp in resps):
            log.exception("response has errors {0}".format(resps))
            raise
        return resps
