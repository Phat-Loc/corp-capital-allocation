# -*- coding: utf-8 -*-
"""

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
    if not es.indices.exists("corp_timeline"):
        text_line_def = {"mappings": {
            "properties": {
                "symbol": {"type": "keyword"},
                "cik": {"type": "integer"},
                "first_date": {"type": "date"},
                "last_date": {"type": "date"},
                "events": {"type": "object"}
            }
        }}

        es.indices.create(index="corp_timeline", body=text_line_def)


def create_timeline(es: elasticsearch.Elasticsearch, cik: int):
    """

    :param es:
    :param cik:
    :return:
    """
    # something is wrong with sorting in elasticsearch
    # s = Search(using=es, index="cap_alloc_event") \
    #     .filter("term", cik=cik) \
    #     .sort('cik', 'as_of_date')

    s = Search(using=es, index="cap_alloc_event") \
        .filter("term", cik=cik)

    unique_sentences = set()

    def score_event(search_term_category: str, capital_allocation: dict, min_prob: float = 0.7):
        """
        Final filter for corp allocation event
        :param search_term_category:
        :param capital_allocation:
        :param min_prob:
        :return:
        """
        cap_alloc_states = sorted(capital_allocation.items(), key=lambda cat: cat[1], reverse=True)
        cap_alloc_state, cap_alloc_state_prob = cap_alloc_states[0]
        if cap_alloc_state_prob < min_prob:
            return cap_alloc_state
        else:
            if cap_alloc_state == search_term_category:
                return cap_alloc_state
            else:
                # Use the search category classifier still needs work
                return search_term_category

                # Boost the search for term category by 10%
                # if cap_alloc_state_prob > capital_allocation[search_term_category] + 0.1:
                #     return cap_alloc_state
                #
                # if cap_alloc_state['search_term_category'] + 0.1 > min_prob:
                #     return search_term_category
                #
                # return 'unknown'

    timeline = {"cik": cik}
    current_period = None
    timeline_periods = []

    docs = [doc for doc in s.scan()]
    docs.sort(key=lambda doc: date.fromisoformat(doc['as_of_date']))

    for corp_alloc_event in docs:
        # There are duplicate sentences
        if corp_alloc_event['content'] not in unique_sentences:
            sentence = corp_alloc_event['content']
            unique_sentences.add(sentence)
            as_of_date = corp_alloc_event['as_of_date']
            # print("{0}: {1}".format(as_of_date, sentence))
            search_term_category = corp_alloc_event['search_term_category']
            capital_allocation = corp_alloc_event['capital_allocation'].to_dict()

            if current_period is None:
                timeline["first_date"] = as_of_date
                current_period = {"start": as_of_date,
                                  "sentence": sentence,
                                  "sentence_count": 1,
                                  "corp_alloc": score_event(search_term_category, capital_allocation)}
            else:
                final_corp_allocation_state = score_event(search_term_category, capital_allocation)
                if final_corp_allocation_state != current_period["corp_alloc"]:
                    # Close the previous period
                    current_period['end'] = as_of_date
                    timeline_periods.append(current_period)

                    # Start a new period
                    current_period = {"start": as_of_date,
                                      "sentence": sentence,
                                      "sentence_count": 1,
                                      "corp_alloc": final_corp_allocation_state}
                else:
                    # Increment the sentence count used to clean up noise later
                    current_period["sentence_count"] += 1

    # Close the final period
    if current_period is not None:
        today = date.today().isoformat()
        timeline["last_date"] = today
        current_period['end'] = today
        timeline_periods.append(current_period)

    timeline["events"] = timeline_periods
    return timeline


def create_timeline_subscribe(es: elasticsearch.Elasticsearch,
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
        content = msg.data().decode('utf-8')
        doc = json.loads(content)
        try:
            cik = doc['cik']
            symbol = doc['symbol']
            company_name = doc['company_name']
            timeline = create_timeline(es, cik)
            timeline['symbol'] = symbol
            timeline['company_name'] = company_name
            es.index(index='corp_timeline', body=timeline, id=symbol)
        except Exception as e:
            _logger.error("Error  create_timeline:{doc}".format(doc=doc) + "\n{0}".format(e))
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
                        default="create-timeline")

    parser.add_argument("-pcs",
                        "--pulsar_connection_string",
                        help="Pulsar connection string e.g. pulsar://localhost:6650",
                        type=str,

                        default="pulsar://10.0.0.11:6650,pulsar://10.0.0.12:6650,pulsar://10.0.0.13:6650")

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
    # create_timeline(es, cik=93410, symbol='CVX')
    create_timeline_subscribe(es=es, sub_topic=args.sub_topic, pulsar_connection_string=args.pulsar_connection_string)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
