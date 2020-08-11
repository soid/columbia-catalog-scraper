# framework for labeling data

import json
import os
from random import random
from typing import List


class Label:
    def __init__(self, in_filename: str, out_filename: str):
        self.input_filename = in_filename
        self.data_file = open(in_filename, 'r')
        self._load_data()

        self.out_filename = out_filename
        self.out_file = open(out_filename, 'a')

    def _load_data(self):
        self.data = []
        line = self.data_file.readline()
        while line != '':
            obj = json.loads(line)
            self.data.append(obj)
            line = self.data_file.readline()

    def select_sample(self) -> dict:
        return random.choice(self.data)

    def print_sample(self, row):
        print(row)

    def print_input_description(self, row):
        """ Optionally describe what input is allowed."""
        pass

    def process_input(self, answer_str, row) -> bool:
        row['label'] = answer_str
        return True

    def print_stat(self):
        pass

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

            self.out_file.write(json.dumps(row) + "\n")

            # output stat
            if num % 5 == 0:
                self.print_stat()
