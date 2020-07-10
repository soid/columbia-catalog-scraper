# -*- coding: utf-8 -*-

import datetime
import os
import difflib
import logging

from columbia_crawler import config
from columbia_crawler.items import ColumbiaClassListing, ColumbiaDepartmentListing

logger = logging.getLogger(__name__)


class StoreRawListeningPipeline(object):

    @staticmethod
    def get_last_file(path):
        files = [fn for fn in os.listdir(path) if os.path.isfile(path + '/' + fn)]
        if len(files) > 0:
            return path + '/' + sorted(files)[-1]
        else:
            return None

    @staticmethod
    def _check_diff(content1, content2):
        seq_differ = difflib.SequenceMatcher()
        seq_differ.set_seqs(content1.split("\n"), content2.split("\n"))
        if seq_differ.ratio() == 1.0:
            # TODO: check it's not only generation date is different
            return True
        return False

    def process_item(self, item, spider):
        if isinstance(item, ColumbiaDepartmentListing):
            self.process_department_listing(item)
        if isinstance(item, ColumbiaClassListing):
            self.process_class_listing(item)

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

        last_file = StoreRawListeningPipeline.get_last_file(out_dir)
        if last_file is not None:
            lf = open(last_file, "r")
            if not self._check_diff(lf.read(), raw_content):
                logger.info("%s listing has the same content. Skipping storing", description)
                return

        f = open(out_file, "w")
        f.write(raw_content)
        f.close()
