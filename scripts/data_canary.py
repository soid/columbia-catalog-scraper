# Validate exported data in:
#   - data/classes
#   - data/instructors (TODO)

from os import listdir
import unittest
import re
from typing import re as regex, List, Callable, Any

import pandas as pd
from cu_catalog import config


class CanaryTest(unittest.TestCase):
    def test_class_files(self):
        json_files_count = 0
        for cls_fn in sorted(listdir(config.DATA_CLASSES_DIR)):
            if cls_fn.endswith('.json'):
                term, _ = cls_fn.rsplit('.', 1)
                self._setTestFileName(cls_fn)
                json_files_count += 1

                df = pd.read_json(config.DATA_CLASSES_DIR + '/' + cls_fn, lines=True)

                # validate fields

                # course_code
                self.assertColRegex(df, 'course_code', r'^[\w_]{4} (\w{1,3}\d{2,4}|\dX+)\w{0,2}_?$', 25)

                self.assertColRegex(df, 'course_title', r'^...+$', 25)

                self.assertColHasUniqueValues(df, 'course_descr', 25)

                self.assertColHasUniqueValues(df, 'instructor', 25)

                # scheduled_time_start and scheduled_time_end
                for col in ['scheduled_time_start', 'scheduled_time_end']:
                    self.assertColRegex(df, col, r'^\d{1,2}:\d{2}(am|pm)$', 2, [''])
                self.assertColRegex(df, 'scheduled_days', r'^(M|T|W|R|F|S|U){1,7}$', 1)

                # call_number
                self.assertColType(df, 'call_number', int, 10)

                # campus
                self.assertColInSet(df, 'campus',
                                    ['Morningside', 'Barnard College', 'Presbyterian Hospital'], 1)

                self.assertColRegex(df, 'class_id', r'^[\w\d]{5,6}-\d{5}-[\w\d]{3}$', 25)

                # department
                self.assertColInSet(df, 'department',
                                    ['Electrical Engineering', 'Computer Science', 'Physics'], 1)
                self.assertColInSet(df, 'department_code',
                                    ['ANTH', 'COMS', 'PHYS', 'HIST', 'POLI', 'RUSS'], 3)

                # TODO: instructor_culpa_link

                # instructor_culpa_nugget
                self.assertColInSet(df, 'instructor_culpa_nugget',
                                    ['gold', 'silver'], 1)

                self.assertColType(df, 'instructor_culpa_reviews_count', float, 10)

                # TODO: instructor_wikipedia_link
                # TODO: link

                if term != '2021-Fall':
                    self.assertColInSet(df, 'location', ['To be announced'])

                # TODO: open_to
                # self.assertColInSet(df, 'open_to', ['DISCUSSION', 'LECTURE'], 1)

                self.assertColRegex(df, 'points', r'^\d{1,2}(\.\d)?(-\d{1,2}(\.\d)?)?$', 4)

                self.assertColInSet(df, 'type', ['DISCUSSION', 'LECTURE', 'SEMINAR'], 1)

        self.assertGreater(json_files_count, 0)

    def assertColRegex(self, df: pd.DataFrame, col: str, regex: regex, min_values: int,
                       exceptions: List[str] or None = None):
        if isinstance(regex, (str, bytes)):
            assert regex, "expected_regex must not be empty."
            regex = re.compile(regex)
        def assert_func(val, msg):
            if not regex.search(val):
                return False
            return True
        self._assertGeneric(df, col, assert_func, min_values, exceptions)

    def assertColInSet(self, df: pd.DataFrame, column: str, expected_values: List[str],
                       min_values: int or None = None,
                       exceptions: List[str] or None = None):
        """expect at least min_values from list expected_values in the column."""
        if min_values is None:
            min_values = len(expected_values)
        assert min_values <= len(expected_values)
        assert min_values != 0

        expected_values_copy = list(expected_values)
        found_values = {}
        found_count = 0

        def assert_func(val, msg):
            nonlocal found_count
            found_values[val] = msg
            if val in expected_values_copy:
                expected_values_copy.remove(val)
                found_count += 1
            return True

        self._assertGeneric(df, column, assert_func, min_values, exceptions)
        self.assertGreaterEqual(found_count, min_values,
                                "Not found at least {} values from the list {} in column: {}, file: {}.\n"
                                'Found values: {}'
                                .format(min_values, expected_values, column, self.test_file_name,
                                        "\n    ".join([str(k) + ": " + str(v) for k, v in found_values.items()])))

    def assertColHasUniqueValues(self, df: pd.DataFrame, col: str, min_values: int):
        pass

    def assertColType(self, df: pd.DataFrame, col: str, expected_type: type, min_distinct_values: int):
        def assert_func(val: int, msg):
            self.assertEqual(expected_type, type(val), msg)
            return True
        self._assertGeneric(df, col, assert_func, min_distinct_values)

    def _setTestFileName(self, filename: str):
        self.test_file_name = filename

    def _assertGeneric(self, df: pd.DataFrame, column: str,
                       assert_func: Callable[[Any, str], None], min_values: int,
                       exceptions: List[str] or None = None):
        count = 0
        for index, row in df.drop_duplicates([column]).iterrows():
            if row[column] is not None \
                    and (exceptions is None or row[column] not in exceptions):
                result = assert_func(row[column], "in column: '{}' file: '{}' coure_code: '{}' link: '{}'"
                            .format(column, self.test_file_name, row['course_code'], row['link']))
                if not result:
                    continue
                count += 1
        self.assertGreaterEqual(count, min_values,
                           "in column: '{}' file: '{}'".format(column, self.test_file_name))


if __name__ == '__main__':
    unittest.main()
