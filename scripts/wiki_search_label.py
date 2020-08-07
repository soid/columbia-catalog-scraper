# Script for manual classification of search results from wikipedia

# The purpose of this classification is to decide if Wiki search result links are relevant to
# a given instructor name. User labels search result as relevant, irrelevant, and possibly relevant.
# Possibly match labels instruct the spider to open the wiki article and decides match / non-match
# based on the article content.

from os.path import dirname, abspath
import sys
CRAWLER_DIR = dirname(dirname(abspath(__file__))) + "/columbia_crawler"
sys.path.insert(0, CRAWLER_DIR)
from columbia_crawler import config
from wiki_search_train import WSC

import json
import random
import os
from termcolor import colored


def colorize(search, text):
    words = search.split()
    for w in words:
        text = text.replace(w, colored(w, 'green'))
    return text


data_file = open(config.DATA_WIKI_FILENAME, 'r')
out_file = open(config.DATA_WIKI_TRAIN_FILENAME, 'a')

data = []
line = data_file.readline()
while line != '':
    obj = json.loads(line)
    data.append(obj)
    line = data_file.readline()

sample = 0
while True:
    sample += 1
    os.system('clear')

    while True:
        row = random.choice(data)
        if random.randint(0, 100) < 10:
            # chance we take any sample
            break
        if len(row['search_results']) == 1:
            # select rows with only one search result - those are most likely relevant or possibly relevant
            break

    print("Searching:    Name:         {} ({} results)".format(row['name'], len(row['search_results'])))
    print("              Department:   {}".format(row['department']))
    print("              Course:       {}".format(row['course_descr']))
    print("\n")
    print("Search results:")
    for i, sr in enumerate(row['search_results']):
        print("    {})".format(i))
        print("    ", colorize(row['name'], sr['title']))
        print("    ", colorize(row['name'], sr['snippet']))
        print("\n")

    reading = True

    def check_input(condition: bool, msg: str):
        if condition:
            print("Error:", msg)
            raise ValueError("Error:" + msg)

    while reading:
        if len(row['search_results']) > 1:
            print("Relevance?   n - nothing is relevant; %n,%n,%n - possibly relevant (e.g. 1,4,5); "
                  "m%n - match (e.g. m1)")
        else:
            print("Relevance?   n - not relevant; p - possibly relevant; m - match")
        answer_str = input()
        answers = list(map(lambda x: x.strip(), answer_str.split(",")))
        # validate
        reading = False
        a: str
        try:
            for a in answers:
                if a == 'n':
                    check_input(len(answers) != 1, "can't use n with other answers")
                    break

                elif a.isnumeric():
                    i = int(a)
                    check_input(i < 0 or i >= len(row['search_results']), "incorrect numeric choice:" + a)
                    row['search_results'][i]['label'] = WSC.LABEL_POSSIBLY

                elif a == 'm':
                    check_input(len(answers) != 1, "can't use n with other answers")
                    check_input(len(row['search_results']) > 1,
                                "ambiguous match. There should be only one matching result.")
                    row['search_results'][0]['label'] = WSC.LABEL_RELEVANT

                elif a == 'p':
                    check_input(len(answers) != 1, "can't use n with other answers")
                    check_input(len(row['search_results']) > 1,
                                "ambiguous match. There should be only one matching result.")
                    row['search_results'][0]['label'] = WSC.LABEL_POSSIBLY

                elif a.startswith("m"):
                    choice = a[1:]
                    check_input(not choice.isnumeric() or int(choice) < 0 or int(choice) >= len(row['search_results']),
                                "incorrect numeric choice:" + choice)
                    check_input(len(answers) != 1, "can't use n with other answers")
                    i = int(choice)
                    row['search_results'][i]['label'] = WSC.LABEL_RELEVANT

                else:
                    check_input(True, "incorrect answer:" + a)

        except ValueError:
            reading = True

    for sr in row['search_results']:
        if 'label' not in sr:
            sr['label'] = 0  # irrelevant result

    out_file.write(json.dumps(row) + "\n")

    if sample % 5 == 0:
        out_file.flush()
        f = open(config.DATA_WIKI_TRAIN_FILENAME, 'r')
        i = 0
        l = f.readline()

        count_match = 0
        count_not_match = 0
        count_possible = 0
        count_total_instructors = 0
        count_total_samples = 0

        while l != '':
            row = json.loads(l)
            count_total_instructors += 1
            for sr in row['search_results']:
                if sr['label'] == WSC.LABEL_IRRELEVANT:
                    count_not_match += 1
                if sr['label'] == WSC.LABEL_POSSIBLY:
                    count_possible += 1
                if sr['label'] == WSC.LABEL_RELEVANT:
                    count_match += 1
                count_total_samples += 1

            l = f.readline()
            i += 1
        os.system('clear')
        print("Total instructors labeled:", count_total_instructors)
        print("Total match samples:", count_match)
        print("Total possible match samples:", count_possible)
        print("Total not match samples:", count_not_match)
        print("Total labeled total samples:", count_total_samples)
        input()
