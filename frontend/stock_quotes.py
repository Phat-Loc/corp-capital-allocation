"""
Handles the getting and storing stock quotes and capital allocation timeline.
"""


import os
import elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Search, Q
from flask import json
import datetime
import requests

IEX_TOKEN = os.getenv('IEX_TOKEN')
IEX_API_VERSION = os.getenv('IEX_API_VERSION')
IEX_URL = "https://cloud.iexapis.com/" if IEX_API_VERSION == 'v1' else 'https://sandbox.iexapis.com/'

ELASTICSEARCH_HOSTS = os.getenv('ELASTICSEARCH_HOSTS')
ELS_CLIENT = None


def get_es_client():
    """
    Factory function to create an Elasticsearch client.
    :return:
    """
    global ELS_CLIENT
    if ELS_CLIENT is None:
        if ELASTICSEARCH_HOSTS is None:
            ELS_CLIENT = elasticsearch.Elasticsearch(['localhost:9200'])
        else:
            ELS_CLIENT = elasticsearch.Elasticsearch(ELASTICSEARCH_HOSTS.split(','))
    return ELS_CLIENT


def init_stock_quotes_hist_idx(es: elasticsearch.Elasticsearch = ELS_CLIENT):
    """
    Makes an index in Elasticsearch if it doesn't already exists.
    :param es:
    :return:
    """
    # Create index if it doesn't exists
    if not es.indices.exists("stock_quotes_hist"):
        index_def = {"mappings": {
            "properties": {
                "symbol": {"type": "keyword"},
                "as_of_date": {"type": "date"},
                "close": {"type": "float"},
                "volume": {"type": "float"}
            }
        }}

        es.indices.create(index="stock_quotes_hist", body=index_def)


def store_stock_quotes_hist(symbol: str, quotes, es: elasticsearch.Elasticsearch = ELS_CLIENT):
    """
    Make sure the quotes have _index set to stock_quotes_hist before requesting to save

    :param symbol:
    :param quotes:
    :param es:
    :return:
    """
    init_stock_quotes_hist_idx(es)

    for quote in quotes:
        quote['_index'] = "stock_quotes_hist"
        quote['_id'] = "{symbol}|{date}".format(symbol=symbol, date=quote['date'])
        quote['symbol'] = symbol
        quote['as_of_date'] = quote['date']
        del quote['date']

    if IEX_API_VERSION == 'v1':
        helpers.bulk(es, quotes)
    else:
        # Don't store the test data
        pass
    return quotes


def store_stock_quotes_hist_from_file(symbol: str, quotes_file: str, es: elasticsearch.Elasticsearch = ELS_CLIENT):
    """
    Helper function to load from a saved file of quotes
    :param symbol:
    :param quotes_file:
    :param es:
    :return:
    """
    with open(quotes_file, 'r') as qf:
        quotes = json.load(qf)
        store_stock_quotes_hist(symbol, quotes, es)


def get_stock_quotes_hist_from_es(symbol: str = 'LPG', es: elasticsearch.Elasticsearch = ELS_CLIENT):
    """
    Search Elasticsearch for the quotes.
    :param symbol:
    :param es:
    :return:
    """

    init_stock_quotes_hist_idx(es)
    s = Search(using=es, index="stock_quotes_hist") \
        .filter("term", symbol=symbol) \
        .sort({"as_of_date": {"order": "asc"}}) \
        .params(request_timeout=300)

    if 0 == s.count():
        return
    else:
        for hit in s.scan():
            yield {"symbol": hit.symbol,
                   "as_of_date": hit.as_of_date,
                   "close": hit.close,
                   "volume": hit.volume}


def handle_eod_historical_prices(symbol: str = 'LPG'):
    """
    Fetches from Elasticsearch if available otherwise queries it from IEX and saves it into Elasticsearch
    :param symbol:
    :return:
    """
    es_client = get_es_client()

    def download_and_store_from_iex(range: str):
        hist_url = IEX_URL + "stable/stock/{symbol}/chart/{range}?token={token}&chartCloseOnly=true".format(symbol=symbol,
                                                                                                      range=range,
                                                                                                      token=IEX_TOKEN)

        r = requests.get(hist_url)
        iex_quotes = json.loads(r.content.decode('utf-8'))
        iex_quotes = store_stock_quotes_hist(symbol.upper(), iex_quotes, es_client)
        return iex_quotes

    stored_quotes = get_stock_quotes_hist_from_es(symbol, es_client)
    quotes = list(stored_quotes)

    if len(quotes) == 0:
        iex_quotes = download_and_store_from_iex('max')
        return iex_quotes

    else:
        if IEX_API_VERSION == 'v1':
            # Only update data if you are in production
            last_quote = quotes[-1]
            start = datetime.datetime.strptime(last_quote['as_of_date'], "%Y-%M-%d").date()
            end = datetime.date.today()
            time_since_last_download = end - start
            if time_since_last_download.days > 1:
                if time_since_last_download.days <= 5:
                    iex_quotes = download_and_store_from_iex('5d')
                elif time_since_last_download.days <= 30:
                    iex_quotes = download_and_store_from_iex('1m')
                else:
                    # Not going to run this pass 3 months
                    iex_quotes = download_and_store_from_iex('3m')
                quotes.extend(iex_quotes)

        return quotes


def get_timeline(symbol: str = 'LPG'):
    """
    Returns a timeline of capital allocation decisions.
    :param symbol:
    :return:
    """
    es = get_es_client()
    res = es.get(index="corp_timeline", id=symbol)
    return res['_source']


def get_symbol_list(symbol_prefix: str ="L"):
    """
    Returns a list of stock symbols that start with the prefix.
    :param symbol_prefix:
    :return:
    """
    es = get_es_client()
    prefix_q = Q("prefix", symbol=symbol_prefix)
    s = Search(using=es, index="corp_timeline")\
        .source(fields=['symbol', 'cik', 'company_name']) \
        .query(prefix_q)

    return [{'id': doc.symbol, 'cik': doc.cik, 'text': doc.company_name} for doc in s.scan()]
