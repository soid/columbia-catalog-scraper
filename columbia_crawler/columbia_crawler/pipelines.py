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
        try:
            if not os.path.exists(path):
                logger.debug(f"Directory {path} does not exist")
                return None
                
            files = [fn for fn in os.listdir(path) if os.path.isfile(path + '/' + fn)]
            logger.debug(f"Found {len(files)} files in {path}")
            
            if len(files) > 0:
                last_file = path + '/' + sorted(files)[-1]
                logger.debug(f"Last file: {last_file}")
                return last_file
            else:
                logger.debug(f"No files found in {path}")
                return None
        except Exception as e:
            logger.error(f"Error getting last file from {path}: {e}")
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
        try:
            seq_differ = difflib.SequenceMatcher()
            seq_differ.set_seqs(content1.split("\n"), content2.split("\n"))
            ratio = seq_differ.ratio()
            logger.debug(f"Content similarity ratio: {ratio:.4f}")
            
            if ratio == 1.0:
                logger.debug("Content is identical (ratio = 1.0)")
                # TODO: check it's not only generation date is different
                return False
            else:
                logger.debug(f"Content is different (ratio = {ratio:.4f})")
                return True
        except Exception as e:
            logger.error(f"Error comparing content: {e}")
            # If we can't compare, assume it's different to be safe
            return True

    def process_item(self, item, spider):
        if not config.DATA_RAW_ENABLED:
            logger.debug("DATA_RAW_ENABLED is False, skipping raw data storage")
            return item
        
        if isinstance(item, ColumbiaDepartmentListing):
            logger.debug(f"Processing department listing: {item['department_code']} for term {item.term_str()}")
            self.process_department_listing(item)
        elif isinstance(item, ColumbiaClassListing):
            logger.debug(f"Processing class listing: {item['course_code']} - {item['course_title']} (Call: {item['call_number']})")
            self.process_class_listing(item)
        else:
            logger.debug(f"Unknown item type: {type(item).__name__}")
        
        return item

    def process_department_listing(self, item):
        out_dir = config.DATA_RAW_DIR + "/" + item.term_str() + "/" + item['department_code']
        logger.info(f"Storing department listing for {item['department_code']} in {out_dir}")
        self._store(out_dir, item['raw_content'], item.describe())
        return item

    def process_class_listing(self, item):
        department_listing = item['department_listing']
        out_dir = config.DATA_RAW_DIR + "/" + department_listing.term_str() \
                  + "/" + department_listing['department_code'] + "/" + item['class_id']
        logger.info(f"Storing class listing for {item['course_code']} (Call: {item['call_number']}) in {out_dir}")
        self._store(out_dir, item['raw_content'], item.describe())
        return item

    def _store(self, out_dir, raw_content, description):
        os.makedirs(out_dir, exist_ok=True)
        out_file = out_dir + "/" + datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H:%M_%Z') + '.html'
        logger.debug(f"Output file path: {out_file}")

        last_file = StoreRawListeningPipeline._get_last_file(out_dir)
        if last_file is not None:
            logger.debug(f"Found previous file: {last_file}")
            try:
                with open(last_file, "r") as lf:
                    last_content = lf.read()
                if not self._is_different(last_content, raw_content):
                    logger.info(f"{description} listing has the same content as previous file. Skipping storage.")
                    return
                else:
                    logger.info(f"{description} listing has different content. Will store new version.")
            except Exception as e:
                logger.error(f"Error reading previous file {last_file}: {e}")
        else:
            logger.info(f"No previous file found for {description}. Storing new content.")

        try:
            with open(out_file, "w") as f:
                f.write(raw_content)
            logger.info(f"Successfully stored {description} to {out_file}")
        except Exception as e:
            logger.error(f"Error writing to file {out_file}: {e}")


class BaseInstructorEnrichmentPipeline(object):
    """Base class for instructor data enrichment

    self.update_fields must be defined as a dictionary: instructor's field -> class field
    """

    def open_spider(self, spider):
        logger.info(f"Opening spider: {spider.name}")
        if os.path.exists(config.DATA_INSTRUCTORS_JSON):
            logger.info(f"Loading existing instructors from {config.DATA_INSTRUCTORS_JSON}")
            try:
                self.instr_df = pd.read_json(config.DATA_INSTRUCTORS_JSON)
                logger.info(f"Loaded {len(self.instr_df)} existing instructors")
            except Exception as e:
                logger.error(f"Error loading instructors file: {e}")
                self.instr_df = pd.DataFrame(dtype=object)
        else:
            logger.info(f"Instructors file not found at {config.DATA_INSTRUCTORS_JSON}, starting with empty DataFrame")
            self.instr_df = pd.DataFrame(dtype=object)

    def close_spider(self, spider):
        logger.info(f"Closing spider: {spider.name}")
        
        # store instructors files
        logger.info(f"Storing {len(self.instr_df)} instructors to {config.DATA_INSTRUCTORS_JSON}")
        try:
            cudata.store_instructors(self.instr_df)
            logger.info("Successfully stored instructors data")
        except Exception as e:
            logger.error(f"Error storing instructors: {e}")
            return

        # Update class files
        df_enriched = self.instr_df.filter(['name'] + list(self.update_fields.keys()))
        logger.info(f"Enriched data contains {len(df_enriched)} instructors with fields: {list(self.update_fields.keys())}")

        columns = dict(self.update_fields)
        columns['name'] = 'instructor'
        df_enriched.rename(columns=columns, inplace=True)
        df_enriched = df_enriched.set_index('instructor')

        if not os.path.exists(config.DATA_CLASSES_DIR):
            logger.warning(f"Classes directory {config.DATA_CLASSES_DIR} does not exist")
            return
            
        files = [fn for fn in os.listdir(config.DATA_CLASSES_DIR) if fn.endswith('.json')]
        logger.info(f"Found {len(files)} class files to update: {files}")
        
        for file in files:
            term, _ = file.rsplit('.', 1)
            logger.info(f"Updating term: {term}")

            # load term file
            try:
                df_term = cudata.load_term(term)
                if df_term is None:
                    logger.warning(f"Could not load term data for {term}")
                    continue
                logger.info(f"Loaded term {term} with {len(df_term)} classes")
            except Exception as e:
                logger.error(f"Error loading term {term}: {e}")
                continue
                
            for class_field in self.update_fields.values():
                if class_field not in df_term.columns:
                    logger.debug(f"Adding missing column {class_field} to term {term}")
                    df_term[class_field] = pd.NaT   # add column if it's not there

            # merge / update wiki links
            logger.debug(f"Merging enriched data with term {term}")
            df_merged = df_term.join(df_enriched, on="instructor", rsuffix='_right')
            
            updates_made = 0
            for class_field in self.update_fields.values():
                before_count = df_merged[class_field].notna().sum()
                df_merged[class_field] = df_merged[class_field].fillna(df_merged[class_field + '_right'])
                after_count = df_merged[class_field].notna().sum()
                updates_made += (after_count - before_count)
                df_merged = df_merged.drop([class_field + '_right'], axis=1)
            
            logger.info(f"Updated {updates_made} fields in term {term}")

            # store
            try:
                StoreClassPipeline.store_classes_term(term, df_merged)
                logger.info(f"Successfully stored updated term {term}")
            except Exception as e:
                logger.error(f"Error storing term {term}: {e}")


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
            logger.debug(f"Processing Wikipedia search results for instructor: {item.get('name', 'Unknown')}")
            s = json.dumps(item.to_dict())
            self.file_wiki_search.write(s + '\n')

        elif isinstance(item, WikipediaInstructorPotentialArticle):
            logger.debug(f"Processing Wikipedia potential article for instructor: {item.get('name', 'Unknown')}")
            s = json.dumps(item.to_dict())
            self.file_wiki_article.write(s + '\n')

        elif isinstance(item, WikipediaInstructorArticle):
            wikipedia_link = 'https://en.wikipedia.org/wiki/' \
                             + quote_plus(item['wikipedia_title'].replace(' ', '_'))
            logger.info(f"Processing Wikipedia article for instructor: {item['name']} -> {wikipedia_link}")

            def instructor_filter(x):
                if x['name'] != item['name']:
                    return False
                deps = item['department'].split('; ')
                return len(set(deps).intersection(set(x['departments']))) > 0

            matching_instructors = self.instr_df.apply(instructor_filter, axis=1)
            matches_count = matching_instructors.sum()
            logger.info(f"Found {matches_count} matching instructors for {item['name']} in departments {item['department']}")
            
            if matches_count > 0:
                self.instr_df.loc[matching_instructors, 'wikipedia_link'] = wikipedia_link
                logger.info(f"Updated Wikipedia link for {matches_count} instructors")
            else:
                logger.warning(f"No matching instructors found for {item['name']} in departments {item['department']}")
        else:
            logger.debug(f"Unknown item type in StoreWikiSearchResultsPipeline: {type(item).__name__}")

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
            logger.info(f"Processing CULPA data for instructor: {item['name']}")
            slice = self.instr_df['name'] == item['name']
            matches_count = slice.sum()
            
            if matches_count > 0:
                logger.info(f"Found {matches_count} matching instructors for {item['name']}")
                self.instr_df.loc[slice, 'culpa_link'] = item['link']
                self.instr_df.loc[slice, 'culpa_nugget'] = item['nugget']
                self.instr_df.loc[slice, 'culpa_reviews_count'] = len(item['reviews'])
                self.instr_df.loc[slice, 'culpa_reviews'] = pd.Series([item['reviews']] * len(slice))
                logger.info(f"Updated CULPA data: {item['link']} with {len(item['reviews'])} reviews")
            else:
                logger.warning(f"No matching instructors found for CULPA data: {item['name']}")
        else:
            logger.debug(f"Unknown item type in StoreCulpaSearchPipeline: {type(item).__name__}")
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
            logger.debug(f"Added class {item['course_code']} (Call: {item['call_number']}) to term {term}")

            if item['instructor']:
                logger.debug(f"Processing instructor data for {item['instructor']} in class {item['course_code']}")
                instr = self.instructors[item['instructor']]
                instr['name'] = item['instructor']
                instr.setdefault('departments', set())
                instr['departments'].add(item['department'])
                instr.setdefault('classes', [])
                cls_code = [term, item['course_code']]
                if cls_code not in instr['classes']:
                    instr['classes'].append(cls_code)
                    logger.debug(f"Added class {cls_code} to instructor {item['instructor']} classes list")

                self.instr2classes[item['instructor']].append(cls)
            else:
                logger.debug(f"No instructor for class {item['course_code']} (Call: {item['call_number']})")
        else:
            logger.debug(f"Unknown item type in StoreClassPipeline: {type(item).__name__}")

        return item

    def close_spider(self, spider):
        logger.info(f"Closing spider: {spider.name}")
        self.classes_files = []
        self.classes_enrollment_files = []

        # store classes
        logger.info(f"Storing classes data to {config.DATA_CLASSES_DIR}")
        os.makedirs(config.DATA_CLASSES_DIR, exist_ok=True)
        os.makedirs(config.DATA_CLASSES_ENROLLMENT_DIR, exist_ok=True)
        
        total_classes = 0
        for term, classes in self.classes_in_term.items():
            logger.info(f"Processing term {term} with {len(classes)} classes")
            total_classes += len(classes)
            
            def _store_term():
                df_json = pd.DataFrame(classes, dtype=object)
                logger.debug(f"Created DataFrame for term {term} with shape {df_json.shape}")
                try:
                    StoreClassPipeline.store_classes_term(term, df_json)
                    logger.info(f"Successfully stored term {term}")
                except Exception as e:
                    logger.error(f"Error storing term {term}: {e}")
                self.classes_files.append(config.DATA_CLASSES_DIR + '/' + term)
                self.classes_enrollment_files.append(config.DATA_CLASSES_ENROLLMENT_DIR + '/' + term)
            _store_term()

        logger.info(f"Total classes processed: {total_classes} across {len(self.classes_in_term)} terms")

        # store instructors
        logger.info(f"Storing {len(self.instructors)} instructors")
        try:
            df_json = pd.DataFrame(self.instructors.values())
            cudata.store_instructors(df_json)
            logger.info("Successfully stored instructors data")
        except Exception as e:
            logger.error(f"Error storing instructors: {e}")

    @staticmethod
    def store_classes_term(term: str, df_json: pd.DataFrame):
        logger.info(f"Storing classes for term {term} with {len(df_json)} classes")
        
        # merge old and new class file
        def _merge_term():
            nonlocal df_json
            logger.debug(f"Loading existing term data for {term}")
            df_old_json = cudata.load_term(term)

            # if department is absent in new, don't remove it, keep old
            # For example, if a department removes an old semester, but other departments still keep it
            if df_old_json is not None:
                logger.info(f"Found existing term data with {len(df_old_json)} classes")
                merge_codes = []
                for dep_code in df_old_json.department_code.unique().tolist():
                    if dep_code not in df_json['department_code'].values:
                        # merge dep_code from old file
                        merge_codes.append(dep_code)
                if len(merge_codes) > 0:
                    logger.info(f"Merging {len(merge_codes)} departments from old data: {merge_codes}")
                    df_json = pd.concat([
                        df_json,
                        df_old_json.loc[df_old_json['department_code'].isin(merge_codes)]
                    ]).reset_index(drop=True)
                    logger.info(f"After merge: {len(df_json)} total classes")
                else:
                    logger.info("No departments to merge from old data")
            else:
                logger.info(f"No existing term data found for {term}")
        _merge_term()

        def _merge_enrollment():
            nonlocal df_json
            logger.debug(f"Merging enrollment data for term {term}")

            # merge enrollment data
            df_enrollment_old_json = StoreClassPipeline._read_term_enrollment(term)
            logger.debug(f"Loaded {len(df_enrollment_old_json)} existing enrollment records")
            
            df2 = df_enrollment_old_json \
                .reset_index() \
                .set_index('call_number')
            df_enrollment_updated = df_json \
                .reset_index() \
                .set_index('call_number')\
                .join(df2, rsuffix='_old', on="call_number")\
                .reset_index()

            logger.debug(f"Enrollment data shape after join: {df_enrollment_updated.shape}")
            
            df_enrollment_updated['enrollment'] = df_enrollment_updated \
                .apply(StoreClassPipeline._merge_enrollment, axis=1)
            df_enrollment_updated = StoreClassPipeline._clean_enrollment(df_enrollment_updated, term)
            df_enrollment_updated = df_enrollment_updated[StoreClassPipeline.ENROLLMENT_COLS]

            # don't store enrollment in main class file
            if 'enrollment' in df_json.columns:
                df_json = df_json.drop(['enrollment'], axis=1)
                logger.debug("Removed enrollment column from main class data")

            return df_enrollment_updated
        df_enrollment_updated = _merge_enrollment()
        df_enrollment_updated.sort_values(by=['course_code', 'call_number'], inplace=True)
        logger.debug(f"Final enrollment data shape: {df_enrollment_updated.shape}")

        # store enrollment in separate files
        if len(df_enrollment_updated) > 0:
            fn = config.DATA_CLASSES_ENROLLMENT_DIR + '/' + term + '.json'
            logger.info(f"Storing enrollment data to {fn}")
            try:
                with open(fn, 'w') as file_json:
                    json_str = df_enrollment_updated.to_json(orient="records", indent=2)
                    # this removes forward slash escaping (due to pandas)
                    file_json.write(json.dumps(json.loads(json_str), ensure_ascii=False, indent=2))
                logger.info(f"Successfully stored enrollment data for {term}")
            except Exception as e:
                logger.error(f"Error storing enrollment data for {term}: {e}")
        else:
            logger.info(f"No enrollment data to store for term {term}")

        # reorder columns
        logger.debug(f"Reordering columns for term {term}")
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

        logger.info(f"Storing final class data for term {term} with {len(df_json)} classes")
        try:
            cudata.store_df(config.DATA_CLASSES_DIR + '/' + term, df_json, df_csv)
            logger.info(f"Successfully stored class data for term {term}")
        except Exception as e:
            logger.error(f"Error storing class data for term {term}: {e}")

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
        try:
            enr_new = row.get('enrollment', {})
            if not enr_new or pd.isna(enr_new):
                enr_new = {}
                
            if 'enrollment_old' not in row:
                logger.debug(f"No old enrollment data found, returning new data with {len(enr_new)} entries")
                return enr_new
                
            enr_old = row['enrollment_old']
            if not enr_old or pd.isna(enr_old):
                enr_old = {}
                
            merged = {**enr_new, **enr_old}
            logger.debug(f"Merged enrollment data: {len(enr_new)} new + {len(enr_old)} old = {len(merged)} total")
            return merged
        except Exception as e:
            logger.error(f"Error merging enrollment data: {e}")
            # Return new data as fallback
            return row.get('enrollment', {})

    @staticmethod
    def _clean_enrollment(df_enrollment_updated: pd.DataFrame, term: str):
        """Remove enrollment data after semester ended"""
        term_end = StoreClassPipeline._get_term_end_date(term)
        logger.debug(f"Cleaning enrollment data for term {term}, end date: {term_end}")

        def _clean(row):
            enr = row['enrollment']
            if len(enr) == 0:
                return {}
            earliest_date = min(enr.keys())
            earliest_data = enr[earliest_date]
            removed_count = 0
            for dt_str in list(enr.keys()):
                try:
                    dt = datetime.date.fromisoformat(dt_str)
                    if dt > term_end:
                        del enr[dt_str]
                        removed_count += 1
                except ValueError as e:
                    logger.warning(f"Invalid date format in enrollment data: {dt_str}, error: {e}")
            if len(enr) == 0:
                # if all data points are outside of semester then just keep the earliest one
                enr[earliest_date] = earliest_data
                logger.debug(f"All enrollment data was outside semester, keeping earliest: {earliest_date}")
            elif removed_count > 0:
                logger.debug(f"Removed {removed_count} enrollment entries after semester end")
            return enr
            
        df_enrollment_updated['enrollment'] = df_enrollment_updated.apply(_clean, axis=1)

        # remove enrollment without any data
        before_count = len(df_enrollment_updated)
        filtered = df_enrollment_updated[df_enrollment_updated['enrollment'] == {}].index
        df_enrollment_updated.drop(filtered, inplace=True)
        after_count = len(df_enrollment_updated)
        
        if before_count != after_count:
            logger.info(f"Removed {before_count - after_count} rows with empty enrollment data")

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
        logger.debug(f"Reading enrollment data from {filename}")
        if not os.path.exists(filename):
            logger.debug(f"Enrollment file {filename} does not exist, returning empty DataFrame")
            return pd.DataFrame(columns=StoreClassPipeline.ENROLLMENT_COLS)
        try:
            df = pd.read_json(filename, dtype=object)
            logger.debug(f"Loaded {len(df)} enrollment records from {filename}")
            return df
        except Exception as e:
            logger.error(f"Error reading enrollment file {filename}: {e}")
            return pd.DataFrame(columns=StoreClassPipeline.ENROLLMENT_COLS)

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
