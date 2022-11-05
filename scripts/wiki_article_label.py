# Script for manual classification of wikipedia articles relation to instructors.
from termcolor import colored

from cu_catalog import config
from cu_catalog.console import colorize
from cu_catalog.models.label import Label


class LabelWikiSearch(Label):
    def print_sample(self, row: dict):
        print("Searching Instructor:")
        print("              Name:         {}".format(row['name']))
        print("              Department:   {}".format(row['department']))
        if row['course_descr']:
            print("              Course:       {}".format(row['course_descr']))
        print()
        print(colored("Wikipedia article (2000 chars):", "yellow"))

        print("Title:", colorize(row['name'], row['wiki_title']))
        page_body = row['wiki_page']
        page_body = page_body[:2000]
        print(colorize(row['name'] + " Columbia", page_body))

    def print_input_description(self, row: dict):
        print()
        print(colored("Is this Wikipedia article about the above instructor? (y/n)", "yellow"))

    def process_input(self, answer_str: str, row: dict) -> bool:
        answer_str = answer_str.lower()
        if answer_str == 'y':
            row['label'] = 1
        elif answer_str == 'n':
            row['label'] = 0
        else:
            return False
        return True

    def update_stat(self, row: dict):
        super().update_stat(row)
        if row['label']:
            self.stat['Relevant samples'] += 1
        elif row['label'] == 0:
            self.stat['Irrelevant samples'] += 1


if __name__ == '__main__':
    labeling = LabelWikiSearch(config.DATA_WIKI_ARTICLE_FILENAME,
                               config.DATA_WIKI_ARTICLE_TRAIN_FILENAME)
    labeling.run()
