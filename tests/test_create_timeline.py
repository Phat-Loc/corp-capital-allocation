import pulsar.sink.create_timeline as timeline


def test_do_create_timeline():
    docs = [{'content': 'Hello world',
             'search_term_category': 'dividend',
             'as_of_date': '2020-02-01',
             'capital_allocation': {'dividend': 0.7,
                                    'share_repurchase': 0.5,
                                    'debt_reduction': 0.5}},
            {'content': 'Hello world2',
             'search_term_category': 'dividend',
             'as_of_date': '2020-02-11',
             'capital_allocation': {'dividend': 0.7,
                                    'share_repurchase': 0.5,
                                    'debt_reduction': 0.5}
             }]
    timeline_doc = timeline.do_create_timeline(999, docs)
    assert timeline_doc['first_date'] == '2020-02-01' and timeline_doc['last_date'] == '2020-02-11'
