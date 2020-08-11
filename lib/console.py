from termcolor import colored


def colorize(search, text):
    words = search.split()
    for w in words:
        text = text.replace(w, colored(w, 'green'))
    return text
