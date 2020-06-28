import re


def split_term(term_str):
    search = re.search('(\w+)(\d{4})', term_str, re.IGNORECASE)
    assert search
    month = search.group(1)
    year = search.group(2)
    return month.capitalize(), year


def normalize_term(term_str):
    """
    Converts term used in URLs to unified form.

    >>> normalize_term("Summer2020")
    '2020-Summer'
    >>> normalize_term("SPRING2020")
    '2020-Spring'
    >>> normalize_term("Fall2019")
    '2019-Fall'
    """
    month, year = split_term(term_str)
    return year + "-" + month
