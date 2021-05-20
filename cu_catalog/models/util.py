import re
import unidecode
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
    and skipping the middle name initial.

    >>> words_match2("Zhi Li", "Ying, Zhiliang")
    False
    >>> words_match2("Zhi Li", "Li, Zhi")
    True
    >>> words_match2("Zhi V Li", "Li, Zhi")
    True
    >>> words_match2("Zhi Li", "Li, Zhi V")
    True
    >>> words_match2("Iuri Bauler Pereira", "Iuri Pereira")
    True
    >>> words_match2("Dragomir R. Radev", "Dragomir Radev")
    True
    >>> words_match2("Dolores Barbazan Capeans", "Dolores Capeáns")
    True
    >>> words_match2("Begona Alberdi", "Begoña Alberdi")
    True
    >>> words_match2("Hee Jin Kim", "Hee-Jin Kim")
    True
    >>> words_match2("Aise Johan de Jong", "Aise de Jongs")
    True
    >>> words_match2("J.C. Salyer", "J.C. Salyer")
    True
    >>> words_match2("Antoni Fernandez Parera", "Antonio Parera")
    True
    >>> words_match2("Julien Dub Dat", "Dubedat, Julien")
    True
    >>> words_match2("Frances Negr N-Muntaner", "Negron-Muntaner, Frances")
    True
    >>> words_match2("Alfred Mac Adam", "MacAdam, Alfred")
    True
    """
    def sort_name(name: str):
        if ',' in name:
            p1, p2 = name.split(',', 1)
            name = p2 + ' ' + p1
            name = re.sub(r'\s+', ' ', name.strip())
        return name
    def name_split(name: str):
        name = re.split('[^\w]', name.lower())
        return [unidecode.unidecode(w) for w in name if len(w) > 1]
    text, search = sort_name(text), sort_name(search)
    text_lst = name_split(text)
    search_lst = name_split(search)
    intersection = set(text_lst).intersection(set(search_lst))
    intersection = len(intersection)
    # if 2 or more name parts match, then count it the same name
    if intersection >= 2 or (len(text_lst) == 1 and intersection == 1):
        return True
    # Try to match similarity
    intersection = 0
    for w1 in text_lst:
        for w2 in search_lst:
            if str_similarity(w1, w2) >= 0.95:
                intersection += 1
                break
    if intersection >= 2 or (len(text_lst) == 1 and intersection == 1):
        return True
    # See if entire names are very similar
    return str_similarity(search, text) >= 0.97

def str_similarity(str1, str2):
    str1, str2 = str1.lower(), str2.lower()
    return textdistance.jaro_winkler.normalized_similarity(str1, str2)
