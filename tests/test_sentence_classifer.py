import frontend.sentence_classifier as classifier


def test_classify_sentence():
    cats = classifier.classify_sentence(
        "These transactions resulted in the removal of approximately $900 million of debt from the company's balance sheet.",
        format="dic")

    assert cats['debt_reduction'] > 0.7
