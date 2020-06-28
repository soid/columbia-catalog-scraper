# -*- coding: utf-8 -*-

import datetime
import os
import difflib
import logging

from columbia_crawler import config

logger = logging.getLogger(__name__)


class StoreRawListeningPipeline(object):

    @staticmethod
    def get_last_file(term_dir):
        files = os.listdir(term_dir)
        if len(files) > 0:
            return term_dir + '/' + sorted(files)[-1]
        else:
            return None

    @staticmethod
    def check_diff(content1, content2):
        seq_differ = difflib.SequenceMatcher()
        seq_differ.set_seqs(content1.split("\n"), content2.split("\n"))
        if seq_differ.ratio() == 1.0:
            # TODO: check it's not only generation date is different
            return True
        return False

    def process_item(self, item, spider):
        out_dir = config.DATA_RAW_DIR + "/" + item.term_str() + "/" + item['department_code']
        os.makedirs(out_dir, exist_ok=True)
        out_file = out_dir + "/" + datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d_%H:%M_%Z') + '.html'

        last_file = StoreRawListeningPipeline.get_last_file(out_dir)
        if last_file is not None:
            lf = open(last_file, "r")
            if not self.check_diff(lf.read(), item['raw_content']):
                logger.info("%s listing has the same content. Skipping storing", item.describe())
                return item

        f = open(out_file, "w")
        f.write(item['raw_content'])
        f.close()

        return item
