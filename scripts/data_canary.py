# Validate exported data in:
#   - data/classes
#   - data/instructors

from os import listdir
import unittest
from typing import re, List, Callable, Any

import pandas as pd
from cu_catalog import config


class CanaryTest(unittest.TestCase):
    def test_class_files(self):
        json_files_count = 0
        for cls_fn in listdir(config.DATA_CLASSES_DIR):
            if cls_fn.endswith('.json'):
                self._setTestFileName(cls_fn)
                json_files_count += 1

                df = pd.read_json(config.DATA_CLASSES_DIR + '/' + cls_fn, lines=True)

                # validate fields

                # course_code
                self.assertColRegex(df, 'course_code', r'^[\w_]{4} (\w{1,3}\d{2,4}|\dX+)\w?_?$', 100)

                self.assertColRegex(df, 'course_title', r'^...+$', 100)

                self.assertColHasUniqueValues(df, 'course_descr', 100)

                self.assertColHasUniqueValues(df, 'instructor', 100)

                # scheduled_time_start and scheduled_time_end
                for col in ['scheduled_time_start', 'scheduled_time_end']:
                    self.assertColRegex(df, col, r'^\d{1,2}:\d{2}(am|pm)$', 10, [''])
                self.assertColRegex(df, 'scheduled_days', r'^(M|T|W|R|F|S|U){1,7}$', 10)

                # call_number
                self.assertColType(df, 'call_number', int, 10)

                # campus
                self.assertColInSet(df, 'campus',
                                    ['Morningside', 'Barnard College', 'Presbyterian Hospital'], 5)

                self.assertColRegex(df, 'class_id', r'^[\w\d]{5}-\d{5}-[\w\d]{3}$', 100)

                # department
                self.assertColInSet(df, 'department',
                                    ['Electrical Engineering', 'Computer Science'], 10)
                self.assertColInSet(df, 'department_code',
                                    ['ANTH', 'COMS', 'PHYS'], 30)

                # instructor_culpa_link

                # instructor_culpa_nugget
                self.assertSetEqual({None, 'gold', 'silver'},
                                    set(df.instructor_culpa_nugget.unique().tolist()))

                self.assertColType(df, 'instructor_culpa_reviews_count', float, 10)

                # instructor_wikipedia_link
                # link

                self.assertColInSet(df, 'location', ['To be announced'], 2)

                # open_to
                # self.assertColInSet(df, 'open_to', ['DISCUSSION', 'LECTURE'], 4)

                self.assertColRegex(df, 'points', r'^\d{1,2}(\.\d)?(-\d{1,2}(\.\d)?)?$', 4)

                self.assertColInSet(df, 'type', ['DISCUSSION', 'LECTURE'], 4)

        self.assertGreater(json_files_count, 0)

    def assertColRegex(self, df: pd.DataFrame, col: str, regex: re, min_values: int,
                       exceptions: List[str] or None = None):
        def assert_func(val, msg):
            self.assertRegex(val, regex, msg)
        self._assertGeneric(df, col, assert_func, min_values, exceptions)

    def assertColInSet(self, df: pd.DataFrame, column: str, expected_values: List[str], min_values: int,
                       exceptions: List[str] or None = None):
        expected_values_copy = list(expected_values)
        found_values = set()

        def assert_func(val, msg):
            found_values.add(val)
            if val in expected_values_copy:
                expected_values_copy.remove(val)

        self._assertGeneric(df, column, assert_func, min_values, exceptions)
        self.assertListEqual([], expected_values_copy,
                             'Not found values in column: {}, file: {}, found values: {}'
                             .format(column, self.test_file_name, found_values))

    def assertColHasUniqueValues(self, df: pd.DataFrame, col: str, min_values: int):
        pass

    def assertColType(self, df: pd.DataFrame, col: str, expected_type: type, min_distinct_values: int):
        def assert_func(val: int, msg):
            self.assertEqual(expected_type, type(val), msg)
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
                assert_func(row[column], "in column: '{}' file: '{}' coure_code: '{}' link: '{}'"
                            .format(column, self.test_file_name, row['course_code'], row['link']))
                count += 1
        self.assertGreater(count, min_values,
                           "in column: '{}' file: '{}'".format(column, self.test_file_name))


if __name__ == '__main__':
    unittest.main()
