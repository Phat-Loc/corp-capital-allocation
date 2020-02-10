# -*- coding: utf-8 -*-
"""
The flask server for the fortune dashboard. Should be run behind a proxy.
"""

from flask import Flask, request, jsonify, render_template
import stock_quotes
import sentence_classifier
import datetime
import elasticsearch

app = Flask(__name__)


@app.route('/classify')
def classify_sentence():
    sentence = request.args.get('sentence')
    if sentence is not None:
        cap_allocation = sentence_classifier.classify_sentence(sentence)
        if cap_allocation is None:
            return 'Unknown'
        else:
            return cap_allocation


@app.route('/chart/<symbol>')
def eod_historical_prices(symbol: str = 'LPG'):
    if "_" in symbol:
        req_symbol, parser = symbol.split("_")
    else:
        req_symbol = symbol
    quotes = stock_quotes.handle_eod_historical_prices(req_symbol.upper())
    return jsonify(quotes)


@app.route('/timeline/<symbol>')
def timeline(symbol: str = 'LPG'):
    if "_" in symbol:
        req_symbol, parser = symbol.split("_")
    else:
        req_symbol = symbol
        parser = 'machine'

    if parser == 'human' and req_symbol == 'LPG':
        # Hand coded timeline for LPG
        events = [
            {
                "start": "2014-05-01",
                "end": "2017-05-31",
                "event": "Organic Growth"
            },
            {
                "start": "2017-06-01",
                "end": "2019-08-06",
                "event": "Debt Reduction"
            },
            {
                "start": "2019-08-07",
                "end": "2020-02-08",
                "event": "Share Buyback"
            }]
    else:
        try:
            display_names = {'debt_reduction': 'Debt Reduction',
                             'dividend': 'Dividend',
                             'mergers_acquisitions': 'Mergers & Acquisitions',
                             'organic_growth': 'Organic Growth',
                             'share_repurchase': 'Share Repurchase',
                             'unknown': 'Unknown'}
            corp_timeline = stock_quotes.get_timeline(req_symbol)
            events = []
            for event in corp_timeline['events']:
                event['event'] = display_names[event['corp_alloc']]
                events.append(event)

        except elasticsearch.exceptions.NotFoundError as e:
            # default answer
            events = [{"start": "2014-01-01",
                       "end": datetime.date.today().isoformat(),
                       "event": "Organic Growth"}]

    return jsonify(events)


@app.route('/lookup')
def lookup():
    """

    :return:
    """
    symbol_prefix = request.args.get('qry')
    results = {'results': stock_quotes.get_symbol_list(symbol_prefix.upper())}
    return jsonify(results)


@app.route('/')
def dash_board():
    return render_template('index.html')
