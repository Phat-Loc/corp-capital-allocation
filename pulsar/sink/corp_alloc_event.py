# -*- coding: utf-8 -*-
"""

"""

import argparse
import sys
import logging
import elasticsearch
import pulsar
import json
import os
import socket
from datetime import date

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"
__version__ = '0.0.1.'

_logger = logging.getLogger(__name__)


def init_els_index(es: elasticsearch.Elasticsearch):
    """
    Use this to define the elsaticsearch index otherwise the system just guesses the data types

    :param es:
    :return:
    """
    if not es.indices.exists("cap_alloc_event"):
        text_line_def = {"mappings": {
            "properties": {
                "content": {"type": "text"},
                "line_number": {"type": "integer"},
                "as_of_date": {"type": "date"},
                "cik": {"type": "integer"},
                "form_type": {"type": "keyword"},
                "text_source_id": {"type": "keyword"},
                "hit_score": {"type": "float"},
                "text_line_id": {"type": "keyword"},
                "search_term": {"type": "keyword"},
                "search_term_category": {"type": "keyword"},
                "capital_allocation": {"type": "object"}
            }
        }}

        es.indices.create(index="cap_alloc_event", body=text_line_def)


def cap_alloc_event_subscribe(es: elasticsearch.Elasticsearch,
                              sub_topic: str = "cap-alloc-event",
                              pulsar_connection_string: str = "pulsar://localhost:6650"):
    """

    :param es:
    :param sub_topic:
    :param pulsar_connection_string:
    :return:
    """
    init_els_index(es)
    client = pulsar.Client(pulsar_connection_string)
    subscription = '{pulsar_topics}-worker'.format(pulsar_topics=sub_topic)
    consumer_name = "pid: {pid} on {hostname}".format(pid=os.getpid(), hostname=socket.gethostname())
    consumer = client.subscribe(topic=sub_topic,
                                subscription_name=subscription,
                                receiver_queue_size=1,
                                max_total_receiver_queue_size_across_partitions=1,
                                consumer_type=pulsar.ConsumerType.Shared,
                                initial_position=pulsar.InitialPosition.Earliest,
                                consumer_name=consumer_name)

    _logger.info("Waiting for message on {topic}".format(topic=sub_topic))
    while True:
        msg = consumer.receive()
        try:
            content = msg.data().decode('utf-8')
            doc = json.loads(content)
            doc["as_of_date"] = date.fromisoformat(doc["as_of_date"])
            es.index(index='cap_alloc_event', body=doc)
        except Exception as e:
            _logger.error("Error cap_alloc_event:{doc}".format(doc=doc) + "\n{0}".format(e))
        consumer.acknowledge(msg)


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="")

    parser.add_argument("-st",
                        "--sub_topic",
                        help="The topic to subscribe to for requests to search timeline of company",
                        type=str,
                        default="cap-alloc-event")

    parser.add_argument("-pcs",
                        "--pulsar_connection_string",
                        help="Pulsar connection string e.g. pulsar://localhost:6650",
                        type=str,
                        default="pulsar://10.0.0.11:6650,pulsar://10.0.0.12:6650,pulsar://10.0.0.13:6650"
                        )

    parser.add_argument("-els",
                        "--elasticsearch_hosts",
                        help="Comma separated elasticsearch hosts e.g. host1,host2,host3",
                        type=str,
                        default='10.0.0.11,10.0.0.12,10.0.0.13'
                        )

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
    if args.loglevel:
        setup_logging(args.loglevel)
    else:
        setup_logging(loglevel=logging.WARNING)

    _logger.info("Starting corp alloc event subscriber")
    elasticsearch_hosts = args.elasticsearch_hosts.split(',')
    es = elasticsearch.Elasticsearch(elasticsearch_hosts)
    cap_alloc_event_subscribe(es=es, sub_topic=args.sub_topic, pulsar_connection_string=args.pulsar_connection_string)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
