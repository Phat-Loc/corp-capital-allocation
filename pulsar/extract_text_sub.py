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
import elasticsearch_dsl
from elasticsearch_dsl.connections import connections
from pathlib import Path

__author__ = "Phat Loc"
__copyright__ = "Phat Loc"
__license__ = "mit"
__version__ = "0.0.1"

_logger = logging.getLogger(__name__)

ENGLISH_WORDS = None
NLTK_TOKENIZER = nltk.data.load('tokenizers/punkt/english.pickle')


class TextSource(elasticsearch_dsl.Document):
    cik = elasticsearch_dsl.Integer()
    form_type = elasticsearch_dsl.Keyword()
    as_of_date = elasticsearch_dsl.Date()
    line_count = elasticsearch_dsl.Integer()
    parse_date = elasticsearch_dsl.Date()
    parser_version = elasticsearch_dsl.Keyword()

    class Index:
        name = 'text_source'


class TextLine(elasticsearch_dsl.Document):
    text_source_id = elasticsearch_dsl.Keyword()
    content = elasticsearch_dsl.Text(analyzer='standard')
    line_number = elasticsearch_dsl.Integer()
    as_of_date = elasticsearch_dsl.Date()

    class Index:
        name = 'text_line'


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


def save_to_elasticsearch(bucket: str, key: str, sentences: list):
    """

    :param bucket:
    :param key:
    :param sentences:
    :return:
    """
    parse_date = datetime.date.today()
    text_source_doc_id = bucket + "|" + key
    cik, form_type, as_of_date, company_name, edgar_file = key.split('|')
    cik = int(cik)
    as_of_date = datetime.datetime.strptime(as_of_date, '%Y%m%d').date()
    line_count = len(sentences)

    text_source = TextSource(meta={'id': text_source_doc_id}, cik=cik, form_type=form_type, as_of_date=as_of_date,
                             line_count=line_count, parse_date=parse_date, parser_version=__version__)
    _logger.info("Saving to text_source id: {id}".format(id=text_source_doc_id))
    text_source.save()
    for line_number, content in enumerate(sentences, 1):
        text_line_doc_id = text_source_doc_id + "|{0}".format(line_number)
        text_line = TextLine(meta={'id': text_line_doc_id}, content=content, line_number=line_number,
                             as_of_date=as_of_date)
        _logger.debug("Saving to text_line id: {id}".format(id=text_line_doc_id))
        text_line.save()


def process_extract_text_req(bucket: str = "dataengine-xyz-edgar-raw-data",
                             key: str = "315852|8-K|20191024|RANGE RESOURCES CORP|edgar/data/315852/0001564590-19-037686.txt"):
    """

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
        save_to_elasticsearch(bucket=bucket, key=key, sentences=sentences)


def extract_text_subscribe(pulsar_topic: str = "extract_text",
                           pulsar_connection_string: str = "pulsar://localhost:6650"):
    """

    :param pulsar_topic:
    :param pulsar_connection_string:
    :return:
    """
    client = pulsar.Client(pulsar_connection_string)

    try:
        subscription = '{0}-worker'.format(pulsar_topic)
        consumer = client.subscribe(pulsar_topic, subscription,
                                    consumer_type=pulsar.ConsumerType.Shared)
        _logger.info("Subscribed to {topic} with {subscription}".format(topic=pulsar_topic, subscription=subscription))
        while True:
            msg = consumer.receive()
            content = msg.data().decode('utf-8')
            _logger.info("Received message '%s' id='%s'", content, msg.message_id())
            req = json.loads(content)
            bucket = req.get('bucket')
            key = req.get('key')
            process_extract_text_req(bucket=bucket, key=key)
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
    parser.add_argument("-pt",
                        "--pulsar_topic",
                        help="Pulsar topic to subscribe to",
                        type=str,
                        default='extract_text')

    parser.add_argument("-pcs",
                        "--pulsar_connection_string",
                        help="Pulsar connection string e.g. pulsar://localhost:6650",
                        type=str,
                        default="pulsar://ip-10-0-0-11.ec2.internal:6650,pulsar://ip-10-0-0-12.ec2.internal:6650,pulsar://ip-10-0-0-13.ec2.internal:6650")

    parser.add_argument("-els",
                        "--elasticsearch_hosts",
                        help="Comma separated elasticsearch hosts e.g. host1,host2,host3",
                        type=str,
                        default='ip-10-0-0-11.ec2.internal,ip-10-0-0-12.ec2.internal,ip-10-0-0-13.ec2.internal')

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
    _logger.debug("Starting extract text subscriber")
    elasticsearch_hosts = args.elasticsearch_hosts.split(',')
    connections.create_connection(hosts=elasticsearch_hosts)
    TextSource.init()
    TextLine.init()
    extract_text_subscribe(args.pulsar_topic, args.pulsar_connection_string)


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    ENGLISH_WORDS = load_word_file('../deps/words/en')
    run()
