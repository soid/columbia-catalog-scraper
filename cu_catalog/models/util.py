from typing import Any

import textdistance
from nltk import word_tokenize, PorterStemmer


class Dict2Vect:
    """ Convert a list of dictionaries into a list of lists (vectors)
    where each index represents certain key from the dictionary.

    >>> d2v = Dict2Vect([{'a': 1, 'b': 2}, {'c': 9, 'd': 99}], absent_value=0)
    >>> d2v.convert({'a': 3})
    [3, 0, 0, 0]
    >>> d2v.convert({'c': 2, 'z': 88})
    [0, 0, 2, 0]
    """
    def __init__(self, data, absent_value: Any = False):
        self.keys = list({k for x in data for k in x.keys()})
        self.keys.sort()
        self.absent_value = absent_value

    def convert(self, row: dict):
        return [row[k] if k in row else self.absent_value for k in self.keys]


stemmer = PorterStemmer()


def extract_word_stems2dict(text, key_prefix="__word_"):
    """Extracts stems for every word and puts in a dict for use as features"""
    features = {}
    tokens = word_tokenize(text)
    for t in tokens:
        features[key_prefix + stemmer.stem(t)] = True
    return features


def words_match(search, text):
    """Match each word from `search` in `text`"""
    text = text.lower()
    search = search.lower()
    for name_part in search.split():
        if name_part not in text:
            return False
    return True


def words_match2(search, text):
    """Same as `words_match` but skip short words - useful for matching names
    and skipping the middle name initial."""
    text = text.lower()
    search = search.lower()
    for name_part in search.split():
        if len(name_part) == 1:
            continue
        if name_part not in text:
            return False
    return True


def str_similarity(str1, str2):
    str1, str2 = str1.lower(), str2.lower()
    return textdistance.jaro_winkler.normalized_similarity(str1, str2)
