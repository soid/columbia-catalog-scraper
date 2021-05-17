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
                logger.info("%s listing has the same content. Skipping storing", description)
                return

        f = open(out_file, "w")
        f.write(raw_content)
        f.close()


class StoreWikiSearchResultsPipeline(object):
    def open_spider(self, spider):
        os.makedirs(config.DATA_WIKI_DIR, exist_ok=True)
        self.file_wiki_search = open(config.DATA_WIKI_SEARCH_FILENAME, 'w')
        self.file_wiki_article = open(config.DATA_WIKI_ARTICLE_FILENAME, 'w')
        if os.path.exists(config.DATA_INSTRUCTORS_JSON):
            self.instr_df = pd.read_json(config.DATA_INSTRUCTORS_JSON, lines=True)
        else:
            self.instr_df = pd.DataFrame()

    def close_spider(self, spider):
        logger.info("Start storing data")
        self.file_wiki_search.close()
        # store instructors files
        StoreClassPipeline.store_instructors(self.instr_df)

        # Update class files
        df_wiki_links = self.instr_df.filter(['name', 'wikipedia_link'])
        df_wiki_links.rename(
            columns={
                'wikipedia_link': 'instructor_wikipedia_link',
                'name': 'instructor'
            },
            inplace=True)
        df_wiki_links = df_wiki_links.set_index('instructor')

        files = [fn for fn in os.listdir(config.DATA_CLASSES_DIR) if fn.endswith('.json')]
        for file in files:
            term, _ = file.rsplit('.', 1)
            logging.info("Updating term: " + term)

            # load term file
            df_term = StoreClassPipeline._read_term(term)
            if 'instructor_wikipedia_link' not in df_term.columns:
                df_term["instructor_wikipedia_link"] = pd.NaT   # add column if it's not there

            # merge / update wiki links
            df_merged = df_term.join(df_wiki_links, on="instructor", rsuffix='_right')
            df_merged['instructor_wikipedia_link'] = df_merged['instructor_wikipedia_link']\
                .fillna(df_merged['instructor_wikipedia_link_right'])
            df_merged = df_merged.drop(['instructor_wikipedia_link_right'], axis=1)

            # store
            StoreClassPipeline.store_classes_term(term, df_merged)
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
            self.instr_df.loc[
                (self.instr_df['name'] == item['name'])
                & self.instr_df['departments'].apply(lambda x: item['department'] in x),
                'wikipedia_link'] = wikipedia_link

        return item


class StoreClassPipeline(object):
    """Store classes and instructors in json and csv formats."""

    def open_spider(self, spider):
        self.classes_in_term: Dict[List[object]] = defaultdict(lambda: [], {})
        self.instructors = defaultdict(lambda: {}, {})

        # mapper from instructor name to classes ref in self.classes_in_term
        self.instr2classes = defaultdict(lambda: [], {})

        self._read_instructors()

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

        if isinstance(item, CulpaInstructor):
            instr = self.instructors[item['name']]
            instr['name'] = item['name']
            instr['culpa_link'] = 'http://culpa.info' + item['link']
            instr['culpa_nugget'] = item['nugget']
            instr['culpa_reviews_count'] = int(item['reviews_count'])

        return item

    def close_spider(self, spider):
        self.classes_files = []

        # update instructors info
        name: str
        clss: List[Dict]
        for name, clss in self.instr2classes.items():
            for cls in clss:
                instr = self.instructors[name]
                if 'culpa_link' in instr and instr['culpa_link']:
                    cls['instructor_culpa_link'] = instr['culpa_link']
                    cls['instructor_culpa_nugget'] = instr['culpa_nugget']
                    cls['instructor_culpa_reviews_count'] = int(instr['culpa_reviews_count'])

        # store classes
        os.makedirs(config.DATA_CLASSES_DIR, exist_ok=True)
        for term, classes in self.classes_in_term.items():
            def _store_term():
                df_json = pd.DataFrame(classes, dtype=object)
                StoreClassPipeline.store_classes_term(term, df_json)
                self.classes_files.append(config.DATA_CLASSES_DIR + '/' + term)
            _store_term()

        # store instructors
        df_json = pd.DataFrame(self.instructors.values())
        StoreClassPipeline.store_instructors(df_json)

    @staticmethod
    def store_classes_term(term: str, df_json: pd.DataFrame):
        # merge old and new: if department is absent in new, don't remove it, keep old
        # For example, if a department removes an old semester, but other departments still keep it
        def _merge():
            nonlocal df_json
            df_old_json = StoreClassPipeline._read_term(term)
            if df_old_json is None:
                return
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
        _merge()

        # sort rows
        df_json.sort_values(by=['course_code', 'course_title', 'call_number'], inplace=True)

        # reorder columns
        df_json = StoreClassPipeline \
            ._change_cols_order(df_json, ['course_code', 'course_title', 'course_descr', 'instructor',
                                          'scheduled_time_start', 'scheduled_time_end'])
        df_json['open_to'] = df_json['open_to'].apply(StoreClassPipeline._to_sorted_list)
        df_json['prerequisites'] = df_json['prerequisites'].apply(StoreClassPipeline._to_sorted_list)

        df_csv = df_json.copy()
        df_csv['open_to'] = df_csv['open_to'] \
            .apply(lambda x: "\n".join(sorted(x)) if np.all(pd.notna(x)) else x)
        df_csv['prerequisites'] = df_csv['prerequisites'] \
            .apply(lambda prereqs:
                                                     "\n".join(sorted([c for cls in prereqs for c in cls]))
                                                     if np.all(pd.notna(prereqs)) else prereqs)

        StoreClassPipeline._store_df(config.DATA_CLASSES_DIR + '/' + term, df_json, df_csv)

    @staticmethod
    def store_instructors(df_json):
        os.makedirs(config.DATA_INSTRUCTORS_DIR, exist_ok=True)
        df_json.sort_values(by=['name'], inplace=True)
        df_json['departments'] = df_json['departments']\
            .apply(StoreClassPipeline._to_sorted_list)
        df_json['classes'] = df_json['classes']\
            .apply(StoreClassPipeline._to_sorted_list)

        df_csv = df_json.copy()
        df_csv['departments'] = df_csv['departments'] \
            .apply(lambda x: "\n".join(sorted(x)) if np.all(pd.notna(x)) else x)
        df_csv['classes'] = df_csv['classes'] \
            .apply(lambda x: ("\n".join([" ".join(sorted(cls)) for cls in x]) if np.all(pd.notna(x)) else x))

        StoreClassPipeline._store_df(config.DATA_INSTRUCTORS_DIR + '/instructors', df_json, df_csv)

    @staticmethod
    def _change_cols_order(df, prioritized_cols):
        cols = sorted(df.columns)
        for n in prioritized_cols:
            cols.remove(n)
        cols = prioritized_cols + cols
        df = df.reindex(cols, axis=1)
        return df

    @staticmethod
    def _store_df(filename: str, df_json: pd.DataFrame, df_csv: pd.DataFrame):
        # store json
        file_json = open(filename + '.json', 'w')
        df_json.to_json(path_or_buf=file_json, orient="records", lines=True)
        file_json.close()

        # store csv
        file_csv = open(filename + '.csv', 'w')
        df_csv.to_csv(path_or_buf=file_csv, index=False)
        file_csv.close()

    def _read_instructors(self):
        if os.path.exists(config.DATA_INSTRUCTORS_JSON):
            with open(config.DATA_INSTRUCTORS_JSON, 'r') as f:
                for line in f:
                    instr = json.loads(line)
                    self.instructors[instr['name']] = instr
                    if instr['departments']:
                        instr['departments'] = set(instr['departments'])

    @staticmethod
    def _read_term(term):
        filename = config.DATA_CLASSES_DIR + '/' + term + '.json'
        if not os.path.exists(filename):
            return
        df = pd.read_json(filename, lines=True, dtype=object)
        return df

    @staticmethod
    def _to_sorted_list(x):
        return sorted(x) if np.all(pd.notna(x)) else x
