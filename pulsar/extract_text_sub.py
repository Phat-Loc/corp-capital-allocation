import argparse
import sys
import logging
import os
from bs4 import BeautifulSoup
import pulsar
import json
import tempfile
import boto3
import nltk.data
import re
import datetime
from pathlib import Path
import elasticsearch
from elasticsearch import helpers
import socket

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"
__version__ = "0.0.1"

_logger = logging.getLogger(__name__)

ENGLISH_WORDS = None
NLTK_TOKENIZER = nltk.data.load('tokenizers/punkt/english.pickle')

__els_init__ = False


DEFAULT_FORM_TYPES = ['8-K',
                      '8-K/A',
                      'SC 13D',
                      'SC 13D/A',
                      'SC 13E3',
                      'SC 13E3/A',
                      '10-Q',
                      '10-Q/A',
                      '10-K',
                      '10-K/A',
                      'DEF 14A',
                      'DEF 14C',
                      'DEFA14C',
                      'DEFC14A',
                      'DEFC14C',
                      'DEFM14A',
                      'DEFM14C',
                      'DEFN14A',
                      'DEFR14A',
                      'DEFR14C',
                      'DEL AM',
                      'DFAN14A',
                      'DFRN14A',
                      'SC 14D9',
                      'SC 14D9/A',
                      'SC 14F1',
                      'SC 14F1/A',
                      'SC TO-C',
                      'SC TO-I',
                      'SC TO-I/A',
                      'SC TO-T',
                      'SC TO-T/A',
                      'SC13E4F',
                      'SC13E4F/A',
                      'SC14D1F',
                      'SC14D1F/A',
                      'SC14D9C',
                      '424A',
                      '424B1',
                      '424B2',
                      '424B3',
                      '424B4',
                      '424B5',
                      '424B7',
                      '424B8',
                      '425',
                      'CB',
                      'CB/A']


def load_word_file(word_file: str):
    """

    :param word_file:
    :return:
    """
    with open(word_file) as f:
        return set(f.read().split('\n'))


def extract_sentences(raw_filing_content: str, bs4_parser: str = 'lxml'):
    """
    Takes the raw filing content of the file and extracts sentences from it
    :param raw_filing_content:
    :param bs4_parser:  Specify the bs4 parser to use default is lxml
    :return:
    """
    soup_html = BeautifulSoup(raw_filing_content, bs4_parser)
    raw_text = soup_html.getText()
    raw_lines = raw_text.split('\n')
    strip_lines = map(lambda l: l.strip(), raw_lines)
    non_empty_lines = filter(lambda l: len(l) > 0, strip_lines)

    def has_whitespace(line: str):
        return any(map(lambda c: c.isspace(), line))

    lines_with_spaces = filter(has_whitespace, non_empty_lines)

    def count_words(line: str):
        words = filter(lambda w: len(w) > 3, line.lower().split(' '))
        words_in_line = set(words)
        count = sum(map(lambda word: word in ENGLISH_WORDS, words_in_line))
        return count

    lines_with_words = filter(lambda l: count_words(l) > 2, lines_with_spaces)
    lines_remove_chars = map(lambda l: l.replace('\xa0', ''), lines_with_words)

    def break_lines_into_sentences(lines):
        for line in lines:
            nltk_sentences = NLTK_TOKENIZER.tokenize(line)
            for st in nltk_sentences:
                for sentence in re.split(r'[.?!]\s?(?=[A-Z])', st):
                    if len(sentence) > 0:
                        if sentence.endswith('.'):
                            yield sentence
                        else:
                            yield sentence + '.'

    # return list(lines_remove_chars)
    sentences = list(break_lines_into_sentences(lines_remove_chars))
    return sentences


def init_els_index(es: elasticsearch.Elasticsearch):
    """
    Use this to define the elsaticsearch index otherwise the system just guesses the data types

    :param es:
    :return:
    """
    if not es.indices.exists("text_source"):
        text_source_def = {"mappings": {
            "properties": {
                "cik": {"type": "integer"},
                "form_type": {"type": "keyword"},
                "as_of_date": {"type": "date"},
                "line_count": {"type": "integer"},
                "parse_date": {"type": "date"},
                "parser_version": {"type": "keyword"}
            }
        }}

        es.indices.create(index="text_source", body=text_source_def)

    if not es.indices.exists("text_line"):
        text_line_def = {"mappings": {
            "properties": {
                "content": {"type": "text"},
                "line_number": {"type": "integer"},
                "as_of_date": {"type": "date"},
                "cik": {"type": "integer"},
                "form_type": {"type": "keyword"}
            }
        }}

        es.indices.create(index="text_line", body=text_line_def)


def save_to_elasticsearch(es: elasticsearch.Elasticsearch, bucket: str, key: str, sentences: list):
    """

    :param es:
    :param bucket:
    :param key:
    :param sentences:
    :return:
    """
    parse_date = datetime.datetime.now()
    text_source_doc_id = bucket + "|" + key
    cik, form_type, as_of_date, company_name, edgar_file = key.split('|')
    cik = int(cik)
    as_of_date = datetime.datetime.strptime(as_of_date, '%Y%m%d').date()
    line_count = len(sentences)

    data = []
    text_source_action = {"cik": cik,
                          "form_type": form_type,
                          "as_of_date": as_of_date,
                          "line_count": line_count,
                          "parse_date": parse_date,
                          "parser_version": __version__}

    res = es.index(index="text_source", body=text_source_action)
    text_source_id = res['_id']
    for line_number, content in enumerate(sentences, 1):
        line_action = {"_index": "text_line",
                       "text_source_id": text_source_id,
                       "content": content,
                       "line_number": line_number,
                       "as_of_date": as_of_date,
                       "cik": cik,
                       "form_type": form_type}
        data.append(line_action)

    _logger.info("Saving to elasticsearch: {text_source_doc_id}".format(text_source_doc_id=text_source_doc_id))
    helpers.bulk(es, data)


def process_extract_text_req(es: elasticsearch.Elasticsearch,
                             bucket: str = "dataengine-xyz-edgar-raw-data",
                             key: str = "315852|8-K|20191024|RANGE RESOURCES CORP|edgar/data/315852/0001564590-19-037686.txt"):
    """

    :param es:
    :param bucket:
    :param key:
    :return:
    """
    s3 = boto3.client('s3')
    with tempfile.SpooledTemporaryFile() as f:
        print("Downloading file: {key}".format(key=key))
        s3.download_fileobj(bucket, key, f)
        f.seek(0)
        sentences = extract_sentences(f.read().decode('utf-8'))
        save_to_elasticsearch(es=es, bucket=bucket, key=key, sentences=sentences)


def extract_text_subscribe(es: elasticsearch.Elasticsearch,
                           pulsar_topics: str = "extract_text",
                           pulsar_connection_string: str = "pulsar://localhost:6650"):
    """

    :param es:
    :param pulsar_topics:
    :param pulsar_connection_string:
    :return:
    """
    client = pulsar.Client(pulsar_connection_string)

    try:
        subscription = '{pulsar_topics}-worker'.format(pulsar_topics=pulsar_topics)
        if ',' in pulsar_topics:
            pts = pulsar_topics.split(',')
        else:
            pts = pulsar_topics

        consumer_name = "pid: {pid} on {hostname}".format(pid=os.getpid(), hostname=socket.gethostname())
        consumer = client.subscribe(topic=pts,
                                    subscription_name=subscription,
                                    receiver_queue_size=1,
                                    max_total_receiver_queue_size_across_partitions=1,
                                    consumer_type=pulsar.ConsumerType.Shared,
                                    initial_position=pulsar.InitialPosition.Earliest,
                                    consumer_name=consumer_name)

        _logger.info("Subscribed to {topic} with {subscription}".format(topic=pulsar_topics, subscription=subscription))
        while True:
            msg = consumer.receive()
            content = msg.data().decode('utf-8')
            _logger.critical("%s received message '%s' id='%s'", consumer_name, content, msg.message_id())
            req = json.loads(content)
            bucket = req.get('bucket')
            key = req.get('key')

            try:
                process_extract_text_req(es=es, bucket=bucket, key=key)
            except Exception as e:
                _logger.error("Error processing bucket:{bucket} key:{key}".format(bucket=bucket, key=key)
                              + "\n{0}".format(e))
            consumer.acknowledge(msg)
    finally:
        client.close()


def process_filing_local_dir(filing_dir: str = "/home/ploc/edgar/RRC"):
    """

    :param filing_dir:
    :return:
    """
    for filing in os.listdir(filing_dir):
        _logger.info("Processing {0}".format(filing))
        with open(os.path.join(filing_dir, filing), 'r') as f:
            sentences = extract_sentences(f.read())
        Path('.').mkdir('output', exist_ok=True)
        with open('output/{0}'.format(filing), 'w') as f:
            f.write('\n'.join(sentences))


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="")

    # default_topics = ["extract-text-{0}".format(form_type) for form_type in DEFAULT_FORM_TYPES]
    parser.add_argument("-pts",
                        "--pulsar_topics",
                        help="comma separated values of topic names",
                        type=str,
                        default="extract-text-8-K,extract-text-8-K-A,extract-text-10-K,extract-text-10-K-A,extract-text-10-Q,extract-text-10-Q-A")

    parser.add_argument("-pcs",
                        "--pulsar_connection_string",
                        help="Pulsar connection string e.g. pulsar://localhost:6650",
                        type=str,
                        default="pulsar://10.0.0.11:6650,pulsar://10.0.0.12:6650,pulsar://10.0.0.13:6650")

    parser.add_argument("-els",
                        "--elasticsearch_hosts",
                        help="Comma separated elasticsearch hosts e.g. host1,host2,host3",
                        type=str,
                        default='10.0.0.11,10.0.0.12,10.0.0.13')

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

    _logger.debug("Starting extract text subscriber")
    elasticsearch_hosts = args.elasticsearch_hosts.split(',')
    es = elasticsearch.Elasticsearch(elasticsearch_hosts)
    init_els_index(es)
    extract_text_subscribe(es, args.pulsar_topics, args.pulsar_connection_string)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    ENGLISH_WORDS = load_word_file('../deps/words/en')
    run()
