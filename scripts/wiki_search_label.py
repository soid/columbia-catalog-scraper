# Script for manual classification of search results from wikipedia

# The purpose of this classification is to decide if Wiki search result links are relevant to
# a given instructor name. User labels search result as relevant, irrelevant, and possibly relevant.
# Possibly match labels instruct the spider to open the wiki article and decides match / non-match
# based on the article content.

import json
import os
import random

from columbia_crawler import config
from console import colorize
from lib.models.wiki_search import WSC
from models.label import Label


class LabelWikiSearch(Label):
    def select_sample(self) -> dict:
        while True:
            row = random.choice(self.data)
            if random.randint(0, 100) < 10:
                # chance we take any sample
                return row
            if len(row['search_results']) == 1:
                # select rows with only one search result - those are most likely relevant or possibly relevant
                return row

    def print_sample(self, row):
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

    def print_input_description(self, row):
        if len(row['search_results']) > 1:
            print("Relevance?   n - nothing is relevant; %n,%n,%n - possibly relevant (e.g. 1,4,5); "
                  "m%n - match (e.g. m1)")
        else:
            print("Relevance?   n - not relevant; p - possibly relevant; m - match")

    def process_input(self, answer_str, row) -> bool:
        answers = list(map(lambda x: x.strip(), answer_str.split(",")))

        # check input
        try:
            for a in answers:
                if a == 'n':
                    self._check_input(len(answers) != 1, "can't use n with other answers")
                    break

                elif a.isnumeric():
                    i = int(a)
                    self._check_input(i < 0 or i >= len(row['search_results']), "incorrect numeric choice:" + a)
                    row['search_results'][i]['label'] = WSC.LABEL_POSSIBLY

                elif a == 'm':
                    self._check_input(len(answers) != 1, "can't use n with other answers")
                    self._check_input(len(row['search_results']) > 1,
                                "ambiguous match. There should be only one matching result.")
                    row['search_results'][0]['label'] = WSC.LABEL_RELEVANT

                elif a == 'p':
                    self._check_input(len(answers) != 1, "can't use n with other answers")
                    self._check_input(len(row['search_results']) > 1,
                                "ambiguous match. There should be only one matching result.")
                    row['search_results'][0]['label'] = WSC.LABEL_POSSIBLY

                elif a.startswith("m"):
                    choice = a[1:]
                    self._check_input(not choice.isnumeric() or int(choice) < 0 or int(choice) >= len(row['search_results']),
                                "incorrect numeric choice:" + choice)
                    self._check_input(len(answers) != 1, "can't use n with other answers")
                    i = int(choice)
                    row['search_results'][i]['label'] = WSC.LABEL_RELEVANT

                else:
                    self._check_input(True, "incorrect answer:" + a)

        except ValueError:
            return False

        for sr in row['search_results']:
            if 'label' not in sr:
                sr['label'] = WSC.LABEL_IRRELEVANT

        return True

    def _check_input(self, condition: bool, msg: str):
        if condition:
            print("Error:", msg)
            raise ValueError("Error:" + msg)

    def print_stat(self):
        f = open(self.input_filename, 'r')
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


if __name__ == '__main__':
    labeling = LabelWikiSearch(config.DATA_WIKI_SEARCH_FILENAME,
                           config.DATA_WIKI_SEARCH_TRAIN_FILENAME)
    labeling.run()
