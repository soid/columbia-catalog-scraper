import datetime
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
        print(config.DATA_CLASSES_DIR)

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

        df_inst_j, df_inst_c, df_cls_j, df_cls_c, df_enrl_j = self._verify_exported_files(pipeline)

        # check two iterations don't change anything
        pipeline = _run_spider(test_items)
        df_inst_j2, df_inst_c2, df_cls_j2, df_cls_c2, df_enrl_j2 = self._verify_exported_files(pipeline)

        assert_frame_equal(df_inst_j, df_inst_j2)

        # check enrollment new date is added
        print('updated:', self.test_class_2['course_code'])
        self.test_class_2['enrollment'] = ColumbiaClassListing._get_enrollment(
            datetime.date(year=2021, month=6, day=10),
            101, 150)

        pipeline = _run_spider(test_items)
        df_inst_j3, df_inst_c3, df_cls_j3, df_cls_c3, df_enrl_j3 = self._verify_exported_files(pipeline)

        assert_frame_equal(df_inst_j, df_inst_j3)
        updated_class = df_enrl_j3[df_enrl_j3['course_code'] == self.test_class_2['course_code']].iloc[0]
        print(updated_class)
        self.assertIn('2021-06-10', updated_class['enrollment'].keys())
        self.assertEqual(2, len(updated_class['enrollment']))
        self.assertEqual(101, updated_class['enrollment']['2021-06-10']['cur'])

        # check enrollment is not updated updated for no change
        self.test_class_2['enrollment'] = ColumbiaClassListing._get_enrollment(
            datetime.date(year=2021, month=6, day=12),
            101, 150)

        pipeline = _run_spider(test_items)
        df_inst_j3, df_inst_c3, df_cls_j3, df_cls_c3, df_enrl_j3 = self._verify_exported_files(pipeline)

        assert_frame_equal(df_inst_j, df_inst_j3)
        updated_class = df_cls_j3[df_cls_j3['course_code'] == self.test_class_2['course_code']].iloc[0]
        # print('updated_class', updated_class['enrollment'])
        # self.assertIn('2021-06-10', updated_class['enrollment'].keys())
        # self.assertEqual(2, len(updated_class['enrollment']))    # the same cur should not be added
        # self.assertEqual(101, updated_class['enrollment']['2021-06-10']['cur'])

    def _get_test_data(self):
        lst = []
        item = ColumbiaClassListing.get_test()
        item['department_listing']['term_year'] = '1920'
        item['department_listing']['term_month'] = 'Spring'
        lst.append(item)

        item = ColumbiaClassListing.get_test()
        item['department_listing']['term_year'] = '1921'
        item['department_listing']['term_month'] = 'Fall'
        w_item = WikipediaInstructorArticle.get_test()
        w_item['name'] = item['instructor']
        lst.append(item)
        lst.append(w_item)

        item = ColumbiaClassListing.get_test()
        self.test_class_2 = item
        item['department_listing']['term_year'] = '1921'
        item['department_listing']['term_month'] = 'Fall'
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

    def _verify_exported_files(self, pipeline):
        """ check generated data """
        # instructors file json
        df_inst_j = pd.read_json(config.DATA_INSTRUCTORS_DIR + '/instructors.json', lines=True)
        self._verify_instructors(df_inst_j)

        # instructor csv
        df_inst_c = pd.read_csv(config.DATA_INSTRUCTORS_DIR + '/instructors.csv')
        self._verify_instructors(df_inst_c)

        self.assertLess(1, len(pipeline.classes_files))

        # class file json
        fall_21_file = next(filter(lambda x: '1921-Fall' in x,
                                   pipeline.classes_files))
        df_cls_j = pd.read_json(fall_21_file + '.json', lines=True)
        self._verify_classes(df_cls_j)

        # class file csv
        df_cls_c = pd.read_csv(fall_21_file + '.csv')
        self._verify_classes(df_cls_c)

        # enrollment file
        fall_21_enrollment_file = next(filter(lambda x: '1921-Fall' in x,
                                              pipeline.classes_enrollment_files))
        df_enrl_j = pd.read_json(fall_21_enrollment_file + '.json', lines=True)

        return df_inst_j, df_inst_c, df_cls_j, df_cls_c, df_enrl_j
