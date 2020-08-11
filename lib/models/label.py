# framework for labeling data

import json
import os
import random
from collections import defaultdict


class Label:
    def __init__(self, in_filename: str, out_filename: str):
        self.print_stat_period = 2
        self.stat = defaultdict(lambda: 0, {})

        self.input_filename = in_filename
        self.data_file = open(in_filename, 'r')
        self._load_data()

        self.out_filename = out_filename
        self.out_file = open(out_filename, 'a')

    def _load_data(self):
        self.data = []
        line = self.data_file.readline()
        while line != '':
            row = json.loads(line)
            self.data.append(row)
            line = self.data_file.readline()

    def _load_stat(self):
        f = open(self.out_filename, 'r')
        line = f.readline()
        while line != '':
            row = json.loads(line)
            self.update_stat(row)
            line = f.readline()
        f.close()

    def select_sample(self) -> dict:
        return random.choice(self.data)

    def print_sample(self, row: dict):
        print(row)

    def print_input_description(self, row: dict):
        """ Optionally describe what input is allowed."""
        pass

    def process_input(self, answer_str: str, row: dict) -> bool:
        row['label'] = answer_str
        return True

    def update_stat(self, row: dict):
        self.stat['Total samples'] += 1

    def print_stat(self):
        os.system('clear')
        print("Statistics for labeled data:")
        for name, stat in self.stat.items():
            print(name + ":", stat)
        input()

    def run(self):
        num = 0
        while True:
            num += 1
            os.system('clear')

            # select sample
            row = self.select_sample()

            # print sample
            self.print_sample(row)

            # print options
            self.print_input_description(row)

            # read input
            while True:
                answer_str = input()
                success = self.process_input(answer_str, row)
                if success:
                    break

            self.update_stat(row)
            self.out_file.write(json.dumps(row) + "\n")

            # output stat
            if num % self.print_stat_period == 0:
                self.print_stat()
