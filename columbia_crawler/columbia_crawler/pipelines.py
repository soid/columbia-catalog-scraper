# -*- coding: utf-8 -*-

import datetime
import json
import os
import difflib
import logging
from urllib.parse import quote_plus
from collections import defaultdict
import numpy as np
import pandas as pd
from typing import Dict, List

from cu_catalog import config
from columbia_crawler.items import ColumbiaClassListing, ColumbiaDepartmentListing, WikipediaInstructorSearchResults, \
    WikipediaInstructorPotentialArticle, WikipediaInstructorArticle, CulpaInstructor
import cu_catalog.models.cudata as cudata

logger = logging.getLogger(__name__)


class StoreRawListeningPipeline(object):

    @staticmethod
    def _get_last_file(path):
        """ Gets the last file from directory (sorted alphabetically)

        >>> last_file = StoreRawListeningPipeline._get_last_file(config.get_testcase_dir("content-diff"))
        >>> type(last_file) == str and len(last_file) > 5
        True
        >>> last_file.endswith("/content-diff/2020-07-10_18:35_UTC.html")
        True
        """
        files = [fn for fn in os.listdir(path) if os.path.isfile(path + '/' + fn)]
        if len(files) > 0:
            return path + '/' + sorted(files)[-1]
        else:
            return None

    @staticmethod
    def _is_different(content1, content2):
        """ Checks if content is different.

        >>> first_file = config.get_testcase_dir("content-diff") + "/2020-06-28_19:37_UTC.html"
        >>> same_file = config.get_testcase_dir("content-diff") + "/2020-07-10_07:03_UTC.html"
        >>> last_file = StoreRawListeningPipeline._get_last_file(config.get_testcase_dir("content-diff"))
        >>> assert first_file != last_file and same_file != last_file and same_file != first_file
        >>> first_content = open(first_file, "r").read()
        >>> same_content = open(same_file, "r").read()
        >>> last_content = open(last_file, "r").read()
        >>> StoreRawListeningPipeline._is_different(first_content, same_content)
        False
        >>> StoreRawListeningPipeline._is_different(first_content, last_content)
        True
        """
        seq_differ = difflib.SequenceMatcher()
        seq_differ.set_seqs(content1.split("\n"), content2.split("\n"))
        if seq_differ.ratio() == 1.0:
            # TODO: check it's not only generation date is different
            return False
        return True

    def process_item(self, item, spider):
        if not config.DATA_RAW_ENABLED:
            # skip, if not enabled
            return item
        if isinstance(item, ColumbiaDepartmentListing):
            self.process_department_listing(item)
        if isinstance(item, ColumbiaClassListing):
            self.process_class_listing(item)
        return item

    def process_department_listing(self, item):
        out_dir = config.DATA_RAW_DIR + "/" + item.term_str() + "/" + item['department_code']
        self._store(out_dir, item['raw_content'], item.describe())
        return item

    def process_class_listing(self, item):
        department_listing = item['department_listing']
        out_dir = config.DATA_RAW_DIR + "/" + department_listing.term_str() \
                  + "/" + department_listing['department_code'] + "/" + item['class_id']
        self._store(out_dir, item['raw_content'], item.describe())
        return item

    def _store(self, out_dir, raw_content, description):
        os.makedirs(out_dir, exist_ok=True)
        out_file = out_dir + "/" + datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H:%M_%Z') + '.html'

        last_file = StoreRawListeningPipeline._get_last_file(out_dir)
        if last_file is not None:
            lf = open(last_file, "r")
            if not self._is_different(lf.read(), raw_content):
                logger.debug("%s listing has the same content. Skipping storing", description)
                return

        f = open(out_file, "w")
        f.write(raw_content)
        f.close()


class BaseInstructorEnrichmentPipeline(object):
    """Base class for instructor data enrichment

    self.update_fields must be defined as a dictionary: instructor's field -> class field
    """

    def open_spider(self, spider):
        if os.path.exists(config.DATA_INSTRUCTORS_JSON):
            self.instr_df = pd.read_json(config.DATA_INSTRUCTORS_JSON)
        else:
            self.instr_df = pd.DataFrame(dtype=object)

    def close_spider(self, spider):
        # store instructors files
        cudata.store_instructors(self.instr_df)

        # Update class files
        df_enriched = self.instr_df.filter(['name'] + list(self.update_fields.keys()))

        columns = dict(self.update_fields)
        columns['name'] = 'instructor'
        df_enriched.rename(columns=columns, inplace=True)
        df_enriched = df_enriched.set_index('instructor')

        files = [fn for fn in os.listdir(config.DATA_CLASSES_DIR) if fn.endswith('.json')]
        for file in files:
            term, _ = file.rsplit('.', 1)
            logger.info("Updating term: " + term)

            # load term file
            df_term = cudata.load_term(term)
            for class_field in self.update_fields.values():
                if class_field not in df_term.columns:
                    df_term[class_field] = pd.NaT   # add column if it's not there

            # merge / update wiki links
            df_merged = df_term.join(df_enriched, on="instructor", rsuffix='_right')
            for class_field in self.update_fields.values():
                df_merged[class_field] = df_merged[class_field].fillna(df_merged[class_field + '_right'])
                df_merged = df_merged.drop([class_field + '_right'], axis=1)

            # store
            StoreClassPipeline.store_classes_term(term, df_merged)


class StoreWikiSearchResultsPipeline(BaseInstructorEnrichmentPipeline):

    def __init__(self):
        self.update_fields = {'wikipedia_link': 'instructor_wikipedia_link'}

    def open_spider(self, spider):
        super(StoreWikiSearchResultsPipeline, self).open_spider(spider)
        # files for storing search results and articles for training classifier
        os.makedirs(config.DATA_WIKI_DIR, exist_ok=True)
        self.file_wiki_search = open(config.DATA_WIKI_SEARCH_FILENAME, 'a')
        self.file_wiki_article = open(config.DATA_WIKI_ARTICLE_FILENAME, 'a')

    def close_spider(self, spider):
        logger.info("Start storing data")
        super(StoreWikiSearchResultsPipeline, self).close_spider(spider)
        self.file_wiki_search.close()
        self.file_wiki_article.close()
        logger.info("Finished storing data")

    def process_item(self, item, spider):
        if isinstance(item, WikipediaInstructorSearchResults):
            s = json.dumps(item.to_dict())
            self.file_wiki_search.write(s + '\n')

        if isinstance(item, WikipediaInstructorPotentialArticle):
            s = json.dumps(item.to_dict())
            self.file_wiki_article.write(s + '\n')

        if isinstance(item, WikipediaInstructorArticle):
            wikipedia_link = 'https://en.wikipedia.org/wiki/' \
                             + quote_plus(item['wikipedia_title'].replace(' ', '_'))

            def instructor_filter(x):
                if x['name'] != item['name']:
                    return False
                deps = item['department'].split('; ')
                return len(set(deps).intersection(set(x['departments']))) > 0

            self.instr_df.loc[
                self.instr_df.apply(instructor_filter, axis=1),
                'wikipedia_link'] = wikipedia_link

        return item


class StoreCulpaSearchPipeline(BaseInstructorEnrichmentPipeline):
    """Store CULPA reviews"""

    def __init__(self):
        self.update_fields = {
            'culpa_link': 'instructor_culpa_link',
            'culpa_nugget': 'instructor_culpa_nugget',
            'culpa_reviews_count': 'instructor_culpa_reviews_count',
        }

    def process_item(self, item, spider):
        if isinstance(item, CulpaInstructor):
            slice = self.instr_df['name'] == item['name']
            self.instr_df.loc[slice, 'culpa_link'] = item['link']
            self.instr_df.loc[slice, 'culpa_nugget'] = item['nugget']
            self.instr_df.loc[slice, 'culpa_reviews_count'] = len(item['reviews'])
            self.instr_df.loc[slice, 'culpa_reviews'] = pd.Series([item['reviews']] * len(slice))
        return item

    def close_spider(self, spider):
        logger.info("Start storing data")
        super(StoreCulpaSearchPipeline, self).close_spider(spider)
        logger.info("Finished storing data")


class StoreClassPipeline(object):
    """Store classes and instructors in json and csv formats."""

    ENROLLMENT_COLS = ['call_number', 'course_code', 'enrollment']

    def open_spider(self, spider):
        self.classes_in_term: Dict[str, List[object]] = defaultdict(lambda: [], {})
        self.instructors = cudata.load_instructors()

        # mapper from instructor name to classes ref in self.classes_in_term
        self.instr2classes = defaultdict(lambda: [], {})

    def process_item(self, item, spider):
        if isinstance(item, ColumbiaClassListing):
            department_listing = item['department_listing']
            term = department_listing.term_str()
            cls = item.to_dict()
            self.classes_in_term[term].append(cls)

            if item['instructor']:
                instr = self.instructors[item['instructor']]
                instr['name'] = item['instructor']
                instr.setdefault('departments', set())
                instr['departments'].add(item['department'])
                instr.setdefault('classes', [])
                cls_code = [term, item['course_code']]
                if cls_code not in instr['classes']:
                    instr['classes'].append(cls_code)

                self.instr2classes[item['instructor']].append(cls)

        return item

    def close_spider(self, spider):
        self.classes_files = []
        self.classes_enrollment_files = []

        # store classes
        os.makedirs(config.DATA_CLASSES_DIR, exist_ok=True)
        os.makedirs(config.DATA_CLASSES_ENROLLMENT_DIR, exist_ok=True)
        for term, classes in self.classes_in_term.items():
            def _store_term():
                df_json = pd.DataFrame(classes, dtype=object)
                StoreClassPipeline.store_classes_term(term, df_json)
                self.classes_files.append(config.DATA_CLASSES_DIR + '/' + term)
                self.classes_enrollment_files.append(config.DATA_CLASSES_ENROLLMENT_DIR + '/' + term)
            _store_term()

        # store instructors
        df_json = pd.DataFrame(self.instructors.values())
        cudata.store_instructors(df_json)

    @staticmethod
    def store_classes_term(term: str, df_json: pd.DataFrame):
        # merge old and new class file
        def _merge_term():
            nonlocal df_json
            df_old_json = cudata.load_term(term)

            # if department is absent in new, don't remove it, keep old
            # For example, if a department removes an old semester, but other departments still keep it
            if df_old_json is not None:
                merge_codes = []
                for dep_code in df_old_json.department_code.unique().tolist():
                    if dep_code not in df_json['department_code'].values:
                        # merge dep_code from old file
                        merge_codes.append(dep_code)
                if len(merge_codes) > 0:
                    df_json = pd.concat([
                        df_json,
                        df_old_json.loc[df_old_json['department_code'].isin(merge_codes)]
                    ]).reset_index(drop=True)
        _merge_term()

        def _merge_enrollment():
            nonlocal df_json

            # merge enrollment data
            df_enrollment_old_json = StoreClassPipeline._read_term_enrollment(term)
            df2 = df_enrollment_old_json \
                .reset_index() \
                .set_index('call_number')
            df_enrollment_updated = df_json \
                .reset_index() \
                .set_index('call_number')\
                .join(df2, rsuffix='_old', on="call_number")\
                .reset_index()

            df_enrollment_updated['enrollment'] = df_enrollment_updated \
                .apply(StoreClassPipeline._merge_enrollment, axis=1)
            df_enrollment_updated = StoreClassPipeline._clean_enrollment(df_enrollment_updated, term)
            df_enrollment_updated = df_enrollment_updated[StoreClassPipeline.ENROLLMENT_COLS]

            # don't store enrollment in main class file
            if 'enrollment' in df_json.columns:
                df_json = df_json.drop(['enrollment'], axis=1)

            return df_enrollment_updated
        df_enrollment_updated = _merge_enrollment()
        df_enrollment_updated.sort_values(by=['course_code', 'call_number'], inplace=True)

        # store enrollment in separate files
        if len(df_enrollment_updated) > 0:
            fn = config.DATA_CLASSES_ENROLLMENT_DIR + '/' + term + '.json'
            file_json = open(fn, 'w')
            if term in ["2023-Fall"]:
                # some files are two large when indented
                df_enrollment_updated.to_json(path_or_buf=file_json, orient="records")
            else:
                df_enrollment_updated.to_json(path_or_buf=file_json, orient="records", indent=2)
            file_json.close()

        # reorder columns
        df_json.sort_values(by=['course_code', 'course_title', 'call_number'], inplace=True)
        df_json = StoreClassPipeline \
            ._change_cols_order(df_json, ['course_code', 'course_title', 'course_descr', 'instructor',
                                          'scheduled_time_start', 'scheduled_time_end'])
        df_json['open_to'] = df_json['open_to'].apply(cudata.to_sorted_list)
        df_json['prerequisites'] = df_json['prerequisites'].apply(cudata.to_sorted_list)

        df_csv = df_json.copy()
        df_csv['open_to'] = df_csv['open_to'] \
            .apply(lambda x: "\n".join(sorted(x)) if np.all(pd.notna(x)) else x)
        df_csv['prerequisites'] = df_csv['prerequisites'] \
            .apply(lambda prereqs:
                                                     "\n".join(sorted([c for cls in prereqs for c in cls]))
                                                     if np.all(pd.notna(prereqs)) else prereqs)

        cudata.store_df(config.DATA_CLASSES_DIR + '/' + term, df_json, df_csv)

    @staticmethod
    def _merge_enrollment(row):
        """
        >>> row = { \
            'enrollment': {'2021-06-12': {'cur': 10, 'max': 15}}, \
            'enrollment_old': {'2021-06-15': {'cur': 9, 'max': 15}} \
        }
        >>> StoreClassPipeline._merge_enrollment(row)
        {'2021-06-12': {'cur': 10, 'max': 15}, '2021-06-15': {'cur': 9, 'max': 15}}
        >>> row = { \
            'enrollment': {'2021-06-12': {'cur': 101, 'max': 150}}, \
            'enrollment_old': {'2021-06-11': {'cur': 101, 'max': 150}, \
                               '2021-06-10': {'cur': 101, 'max': 150}, \
                               '2021-06-08': {'cur':  12, 'max': 113} \
                               } \
            }
        >>> StoreClassPipeline._merge_enrollment(row)
        {'2021-06-12': {'cur': 101, 'max': 150}, '2021-06-11': {'cur': 101, 'max': 150}, '2021-06-10': {'cur': 101, 'max': 150}, '2021-06-08': {'cur': 12, 'max': 113}}
        """
        enr_new = row['enrollment']
        if not enr_new or pd.isna(enr_new):
            enr_new = {}
        if 'enrollment_old' not in row:
            return enr_new
        enr_old = row['enrollment_old']
        if not enr_old or pd.isna(enr_old):
            enr_old = {}
        return {**enr_new, **enr_old}

    @staticmethod
    def _clean_enrollment(df_enrollment_updated: pd.DataFrame, term: str):
        """Remove enrollment data after semester ended"""
        term_end = StoreClassPipeline._get_term_end_date(term)

        def _clean(row):
            enr = row['enrollment']
            if len(enr) == 0:
                return {}
            earliest_date = min(enr.keys())
            earliest_data = enr[earliest_date]
            for dt_str in list(enr.keys()):
                dt = datetime.date.fromisoformat(dt_str)
                if dt > term_end:
                    del enr[dt_str]
            if len(enr) == 0:
                # if all data points are outside of semester then just keep the earliest one
                enr[earliest_date] = earliest_data
            return enr
        df_enrollment_updated['enrollment'] = df_enrollment_updated.apply(_clean, axis=1)

        # remove enrollment without any data
        filtered = df_enrollment_updated[df_enrollment_updated['enrollment'] == {}].index
        df_enrollment_updated.drop(filtered, inplace=True)

        return df_enrollment_updated

    @staticmethod
    def _change_cols_order(df, prioritized_cols):
        cols = sorted(df.columns)
        for n in prioritized_cols:
            cols.remove(n)
        cols = prioritized_cols + cols
        df = df.reindex(cols, axis=1)
        return df

    @staticmethod
    def _read_term_enrollment(term):
        filename = config.DATA_CLASSES_ENROLLMENT_DIR + '/' + term + '.json'
        if not os.path.exists(filename):
            return pd.DataFrame(columns=StoreClassPipeline.ENROLLMENT_COLS)
        df = pd.read_json(filename, dtype=object)
        return df

    @staticmethod
    def _get_term_end_date(term: str) -> datetime.date:
        """
        This function determines (roughly) when semester ends is used for stopping collecting enrollment data.
        :param term:    string like '2021-Fall'

        >>> StoreClassPipeline._get_term_end_date('2020-Fall')
        datetime.date(2020, 12, 26)
        """
        year, semester = term.split('-', 1)
        year = int(year)
        semester = semester.lower()
        if semester == 'spring':
            return datetime.date(year, 5, 20)
        elif semester == 'summer':
            return datetime.date(year, 8, 20)
        elif semester == 'fall':
            return datetime.date(year, 12, 26)
