import pulsar.sink.extract_text as et


def test_extract_sentences():
    with open('data/0001564590-19-037686.txt', 'r') as f:
        raw_filing = f.read()
        sentences = et.extract_sentences(raw_filing)
    assert len(sentences) > 0
