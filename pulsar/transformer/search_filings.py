# -*- coding: utf-8 -*-
"""
Subscribes to requests to search 8-K filings for sentences that can be used to classify capital allocation state of firm.
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
__version__ = "0.0.1"

_logger = logging.getLogger(__name__)

SHARE_REPURCHASE_TERMS = ["announced authorized approved new equity shares stock repurchase buyback program"]

MERGERS_ACQUISITIONS_TERMS = [
    "announced authorized approved definitive binding offer to acquire purchase takeover vote  proxy prospectus proposed combination bolt on merger all outstanding shares common stock"]

DIVIDEND_TERMS = ["pay cash declared increase special authorized approved extraordinary dividend"]

ORGANIC_GROWTH_TERMS = ["reinvest invest key growth initiatives expansion product fleet global",
                        "open new stores build product market", "seeks out opportunities",
                        "organic growth drive future reinvest sustainable revenue aggressive expansion",
                        "international expansion geographically", "ramp up",
                        "took delivery vessel"]

DEBT_REDUCTION_TERMS = ["reduce redemption extinguish lower debt borrowing cost leverage",
                        "restructuring strengthen de-risk balance sheet",
                        "reduction salaries annual bonus layoff",
                        "reduction in force", "lower reduce sg&a"]


def search_terms(cik: int, es: elasticsearch.Elasticsearch, min_score=12):
    """
    Search indexed sentences for search terms related to capital allocation
    :param cik:
    :param es:
    :param min_score:
    :return:
    """

    def create_search_for_term(search_term: str):
        """
        The name says it all create search term for query.
        :param search_term:
        :return:
        """
        s = Search(using=es, index="text_line") \
            .filter("term", cik=cik) \
            .filter("term", form_type='8-K') \
            .query("match", content=search_term) \
            .exclude("match", content="suspended") \
            .exclude("match", content="terminated") \
            .exclude("match", content="completed") \
            .exclude("match", content="bonus") \
            .exclude("match", content="incentive plan") \
            .exclude("match", content="annual meeting") \
            .params(request_timeout=300)
        return s, search_term

    def execute_search(s: Search, search_term: str, search_term_category: str, batch_limit=100):
        """
        Query in batches because scan doesn't produce a score. Yield only the ones that have high scores.
        :param s:
        :param search_term:
        :param search_term:
        :param batch_limit:
        :return:
        """
        count = s.count()
        if count == 0:
            return

        batch_size = min(count, batch_limit)
        i = 0
        while i < count:
            batch = s[i:i + batch_size]
            results = batch.execute()
            for hit in results:
                i += 1
                if hit.meta.score > min_score:
                    words = hit.content.split()
                    if len(words) < 5 or len(words) > 50:
                        # Probably a header or non pulverized
                        pass
                    else:
                        yield {"content": hit.content,
                               "line_number": hit.line_number,
                               "as_of_date": hit.as_of_date,
                               "cik": hit.cik,
                               "form_type": hit.form_type,
                               "text_source_id": hit.text_source_id,
                               "hit_score": hit.meta.score,
                               "text_line_id": hit.meta.id,
                               "search_term": search_term,
                               "search_term_category": search_term_category}
                else:
                    return
        return

    def add_search_term_category(search_term_category: str, search_terms: list):
        return [(search_term_category, search_term) for search_term in search_terms]

    terms = add_search_term_category('share_repurchase', SHARE_REPURCHASE_TERMS) + \
            add_search_term_category('mergers_acquisitions', MERGERS_ACQUISITIONS_TERMS) + \
            add_search_term_category('dividend', DIVIDEND_TERMS) + \
            add_search_term_category('organic_growth', ORGANIC_GROWTH_TERMS) + \
            add_search_term_category('debt_reduction', DEBT_REDUCTION_TERMS)

    for term_category, term in terms:
        _logger.info("Executing search for {term} filter by {cik}".format(term=term, cik=cik))
        cap_alloc_search, qry_term = create_search_for_term(term)
        for sentence in execute_search(cap_alloc_search, qry_term, term_category):
            yield sentence

    return


def process_cik(cik: int, producer: pulsar.Producer, es: elasticsearch.Elasticsearch, min_score=10):
    """
    For an individual company search all the search terms.
    :param cik:
    :param producer:
    :param es:
    :param min_score:
    :return:
    """

    for sentence in search_terms(cik, es, min_score):
        msg = json.dumps(sentence).encode('utf-8')
        producer.send(msg)


def search_sentences_subscribe(es: elasticsearch.Elasticsearch,
                               sub_topic: str = "search_filings-8-K",
                               pub_topic: str = "classify-sentence",
                               pulsar_connection_string: str = "pulsar://localhost:6650"):
    """

    :param es:
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
                                    receiver_queue_size=1,
                                    max_total_receiver_queue_size_across_partitions=1,
                                    consumer_type=pulsar.ConsumerType.Shared,
                                    initial_position=pulsar.InitialPosition.Earliest,
                                    consumer_name=consumer_name)
    _logger.info("Subscribed to {topic} with {subscription}".format(topic=pub_topic, subscription=pub_subscription))

    sub_subscription = '{pulsar_topics}-worker'.format(pulsar_topics=sub_topic)
    # This is a long running taks so set the queue size to 1 do no single worker hogs all the work.
    cik_consumer = client.subscribe(topic=sub_topic,
                                    subscription_name=sub_subscription,
                                    receiver_queue_size=1,
                                    max_total_receiver_queue_size_across_partitions=1,
                                    consumer_type=pulsar.ConsumerType.Shared,
                                    initial_position=pulsar.InitialPosition.Earliest,
                                    consumer_name=consumer_name)

    _logger.critical("Waiting for message to arrive on {sub_topic}".format(sub_topic=sub_topic))
    while True:
        msg = cik_consumer.receive()
        content = msg.data().decode('utf-8')
        _logger.critical("%s received message '%s' id='%s'", consumer_name, content, msg.message_id())
        req = json.loads(content)
        cik = req.get('cik')
        _logger.critical("Processing cik:{cik}'".format(cik=cik))
        try:
            process_cik(cik=cik, producer=producer, es=es)
            if pub_consumer is not None:
                pub_consumer.close()
                pub_consumer = None

        except Exception as e:
            _logger.error("Error processing bucket:{bucket}".format(bucket=cik) + "\n{0}".format(e))
        cik_consumer.acknowledge(msg)


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
                        help="The topic to subscribe to for requests to search timeline of company",
                        type=str,
                        default="classify-sentence")

    parser.add_argument("-st",
                        "--sub_topic",
                        help="The topic to subscribe to for requests to search timeline of company",
                        type=str,
                        default="search_filings-8-K")

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

    _logger.debug("Starting search filings subscriber")
    elasticsearch_hosts = args.elasticsearch_hosts.split(',')
    es = elasticsearch.Elasticsearch(elasticsearch_hosts)
    search_sentences_subscribe(es=es, sub_topic=args.sub_topic, pub_topic=args.pub_topic,
                               pulsar_connection_string=args.pulsar_connection_string)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
