# -*- coding: utf-8 -*-
"""
Used to publish a list of companies to either build the timeline or start the searching the filings to classify.
"""

import argparse
import sys
import logging
import elasticsearch
from elasticsearch_dsl import Search
import pulsar
import json
import os
import socket

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"
__version__ = '0.0.1.'

_logger = logging.getLogger(__name__)


def corp_desc_list_cik(es: elasticsearch.Elasticsearch):
    """
        List all the cik in corp_desc i.e. ones with trading symbol
    :param es:
    :return:
    """
    s = Search(using=es, index="corp_desc")
    return [{'cik': doc.meta.id, 'symbol': doc.symbol, 'company_name': doc.name} for doc in s.scan()]


def publish_companies(corps: list, pub_topic: str = "search_filings-8-K",
                      pulsar_connection_string: str = "pulsar://localhost:6650"):
    """

    :param corps:
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

    i = 1
    for corp in corps:
        _logger.info("Publishing {0} of {1}".format(i, len(corps)))
        msg = json.dumps(corp).encode('utf-8')
        producer.send(msg)
        i += 1


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="This will publish ciks of firms with a trading symbol. Use search_filings-8-K or create-timeline")

    parser.add_argument("-pt",
                        "--pub_topic",
                        help="Publish to search_filings-8-K for search filings or create-timeline to create timeline",
                        type=str,
                        default="search_filings-8-K"
                        # default="create-timeline"
                        )

    parser.add_argument("-pcs",
                        "--pulsar_connection_string",
                        help="Pulsar connection string e.g. pulsar://10.0.0.11:6650,pulsar://10.0.0.12:6650,pulsar://10.0.0.13:6650",
                        type=str,
                        default="pulsar://10.0.0.11:6650,pulsar://10.0.0.12:6650,pulsar://10.0.0.13:6650")

    parser.add_argument("-els",
                        "--elasticsearch_hosts",
                        help="Comma separated elasticsearch hosts e.g. 10.0.0.11,10.0.0.12,10.0.0.13",
                        type=str,
                        default='10.0.0.11,10.0.0.12,10.0.0.13')

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

    _logger.info("Finding all corps and publishing request to {0}".format(args.pub_topic))
    elasticsearch_hosts = args.elasticsearch_hosts.split(',')
    es = elasticsearch.Elasticsearch(elasticsearch_hosts)

    test_firms = [{'cik': 93410, 'symbol': 'CVX', 'company_name': 'CHEVRON CORP'},
                  {'cik': 1596993, 'symbol': 'LPG', 'company_name': 'DORIAN LPG LTD.'}]

    firms = corp_desc_list_cik(es)
    publish_companies(firms, pub_topic=args.pub_topic, pulsar_connection_string=args.pulsar_connection_string)
    _logger.info("Publishing complete")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
