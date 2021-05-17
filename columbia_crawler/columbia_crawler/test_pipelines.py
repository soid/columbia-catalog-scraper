import random
import shutil
from unittest import TestCase

import pandas as pd
from pandas._testing import assert_frame_equal

from columbia_crawler.items import ColumbiaClassListing, WikipediaInstructorArticle, CulpaInstructor
from columbia_crawler.pipelines import StoreClassPipeline
from columbia_crawler.spiders.catalog import CatalogSpider
from cu_catalog import config

random.seed(42)


class TestStoreClassPipeline(TestCase):
    def test_pipeline(self):
        # cleanup
        shutil.rmtree(config.DATA_INSTRUCTORS_DIR, ignore_errors=True)
        shutil.rmtree(config.DATA_CLASSES_DIR, ignore_errors=True)

        def _run_spider(mock_items):
            spider = CatalogSpider()
            scp = StoreClassPipeline()
            scp.open_spider(spider)

            for item in mock_items:
                scp.process_item(item, spider)

            scp.close_spider(spider)
            return scp

        test_items = self._get_test_data()
        pipeline = _run_spider(test_items)

        # check generated data
        def _verify_exported_files():
            df_inst_j = pd.read_json(config.DATA_INSTRUCTORS_DIR + '/instructors.json', lines=True)
            self._verify_instructors(df_inst_j)

            df_inst_c = pd.read_csv(config.DATA_INSTRUCTORS_DIR + '/instructors.csv')
            self._verify_instructors(df_inst_c)

            self.assertLess(1, len(pipeline.classes_files))

            df_cls_j = pd.read_json(pipeline.classes_files[0] + '.json', lines=True)
            self._verify_classes(df_cls_j)

            df_cls_c = pd.read_csv(pipeline.classes_files[0] + '.csv')
            self._verify_classes(df_cls_c)

            return df_inst_j, df_inst_c, df_cls_j, df_cls_c
        df_inst_j, df_inst_c, df_cls_j, df_cls_c = _verify_exported_files()

        # check two iterations don't change anything
        pipeline = _run_spider(test_items)
        df_inst_j2, df_inst_c2, df_cls_j2, df_cls_c2 = _verify_exported_files()

        assert_frame_equal(df_inst_j, df_inst_j2)

    def _get_test_data(self):
        lst = []
        item = ColumbiaClassListing.get_test()
        lst.append(item)

        item = ColumbiaClassListing.get_test()
        w_item = WikipediaInstructorArticle.get_test()
        w_item['name'] = item['instructor']
        lst.append(item)
        lst.append(w_item)

        item = ColumbiaClassListing.get_test()
        c_item = CulpaInstructor.get_test()
        c_item['name'] = item['instructor']
        lst.append(item)
        lst.append(c_item)

        return lst

    def _verify_instructors(self, df):
        self.assertEqual(3, len(df.index))
        self.assertEqual(3, len(df.loc[df['name'].str.contains('test instructor')].index))

    def _verify_classes(self, df):
        self.assertLess(0, len(df.index))
        self.assertLess(1, len(df.loc[df['course_code'].str.contains('test course_code')].index))
