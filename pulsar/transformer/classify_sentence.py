# -*- coding: utf-8 -*-
"""
Subscribes to messages to classify sentences
"""

import argparse
import sys
import logging
import pulsar
import os
import socket
import sentence_classifier
import json

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"
__version__ = "0.0.1"

_logger = logging.getLogger(__name__)


def classify_sentences_subscribe(sub_topic: str = "classify-sentence",
                                 pub_topic: str = "cap-alloc-event",
                                 pulsar_connection_string: str = "pulsar://localhost:6650"):
    """

    :param sub_topic:
    :param pub_topic:
    :param pulsar_connection_string:
    :return:
    """
    client = pulsar.Client(pulsar_connection_string)
    producer = client.create_producer(topic=pub_topic,
                                      block_if_queue_full=True,
                                      batching_enabled=True,
                                      send_timeout_millis=300000,
                                      batching_max_publish_delay_ms=120000)

    # Subscribe so the messages don't get delete if there are no live subscribers before you start
    pub_subscription = '{pulsar_topics}-worker'.format(pulsar_topics=pub_topic)
    consumer_name = "pid: {pid} on {hostname}".format(pid=os.getpid(), hostname=socket.gethostname())
    pub_consumer = client.subscribe(topic=pub_topic,
                                    subscription_name=pub_subscription,
                                    #receiver_queue_size=1,
                                    #max_total_receiver_queue_size_across_partitions=1,
                                    consumer_type=pulsar.ConsumerType.Shared,
                                    initial_position=pulsar.InitialPosition.Earliest,
                                    consumer_name=consumer_name)

    _logger.info("Subscribed to {topic} with {subscription}".format(topic=pub_topic, subscription=pub_subscription))
    sub_subscription = '{pulsar_topics}-worker'.format(pulsar_topics=sub_topic)
    sentence_consumer = client.subscribe(topic=sub_topic,
                                         subscription_name=sub_subscription,
                                         #receiver_queue_size=1,
                                         #max_total_receiver_queue_size_across_partitions=1,
                                         consumer_type=pulsar.ConsumerType.Shared,
                                         initial_position=pulsar.InitialPosition.Earliest,
                                         consumer_name=consumer_name)

    _logger.info("Waiting for message on {topic}".format(topic=sub_topic))
    while True:
        incoming_msg = sentence_consumer.receive()
        content = incoming_msg.data().decode('utf-8')
        req = json.loads(content)
        sentence = req.get('content')

        try:
            capital_allocation_cats = sentence_classifier.classify_sentence(sentence, format='dict')
            req['capital_allocation'] = capital_allocation_cats
            outgoing_msg = json.dumps(req).encode('utf-8')
            producer.send(outgoing_msg)

            if pub_consumer is not None:
                # Close it because we just started it to create a subscription if there wasn't one already.
                pub_consumer.close()
                pub_consumer = None

        except Exception as e:
            _logger.error("Error processing bucket:{bucket}".format(bucket=content) + "\n{0}".format(e))
        sentence_consumer.acknowledge(incoming_msg)


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="")
    parser.add_argument("-pt",
                        "--pub_topic",
                        help="The topic to publish capital allocation events",
                        type=str,
                        default="cap-alloc-event")

    parser.add_argument("-st",
                        "--sub_topic",
                        help="The topic to subscribe to for requests to classify sentences",
                        type=str,
                        default="classify-sentence")

    parser.add_argument("-pcs",
                        "--pulsar_connection_string",
                        help="Pulsar connection string e.g. pulsar://localhost:6650",
                        type=str,
                        # default="pulsar://ec2-34-236-152-169.compute-1.amazonaws.com:6650,pulsar://ec2-3-88-114-93.compute-1.amazonaws.com:6650,pulsar://ec2-18-234-231-226.compute-1.amazonaws.com:6650"
                        default="pulsar://10.0.0.11:6650,pulsar://10.0.0.12:6650,pulsar://10.0.0.13:6650")

    parser.add_argument(
        "--version",
        action="version",
        version="sub-template {ver}".format(ver=__version__))

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO)

    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG)
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


def main(args):
    """Main entry point allowing external calls

    Args:
      args ([str]): command line parameter list
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    _logger.info("Starting classify sentence subscriber")
    classify_sentences_subscribe(sub_topic=args.sub_topic, pub_topic=args.pub_topic,
                                 pulsar_connection_string=args.pulsar_connection_string)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
