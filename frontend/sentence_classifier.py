import spacy

NLP = spacy.load('corp_alloc_bal')


def classify_sentence(sentence: str, min_prob: float = 0.7, format: str = 'text'):
    """
    Classifies the sentence into one of five capital allocation categories or Unknown
    :param sentence:
    :param min_prob: Min cutoff probability
    :return:
    """
    doc = NLP(sentence)
    cap_alloc_states = sorted(doc.cats.items(), key=lambda cat: cat[1], reverse=True)
    if format == 'text':
        state, prob = cap_alloc_states[0]
        if prob > min_prob:
            return "{state} ({prob})".format(state=state, prob='{:.2%}'.format(prob))
        else:
            return "unknown ({prob})".format(prob=prob)
    else:
        return doc.cats

