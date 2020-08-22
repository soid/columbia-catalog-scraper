from unittest import TestCase
import pandas as pd
from pandas._testing import assert_frame_equal

from columbia_crawler.items import ColumbiaClassListing, WikipediaInstructorArticle
from columbia_crawler.pipelines import StoreClassPipeline
from columbia_crawler.spiders.catalog import CatalogSpider
from cu_catalog import config


class TestStoreClassPipeline(TestCase):
    def test_pipeline(self):
        def _run_spider(mock_items):
            spider = CatalogSpider()
            scp = StoreClassPipeline()
            scp.open_spider(spider)

            for item in mock_items:
                scp.process_item(item, spider)

            scp.close_spider(spider)
            return scp

        class_mock_item = ColumbiaClassListing.get_test()
        wiki_mock_item = WikipediaInstructorArticle.get_test()
        pipeline = _run_spider([class_mock_item, wiki_mock_item])

        # check generated data
        def _verify_exported_files():
            df_inst_j = pd.read_json(config.DATA_INSTRUCTORS_DIR + '/instructors.json', lines=True)
            self._verify_instructors(df_inst_j)

            df_inst_c = pd.read_csv(config.DATA_INSTRUCTORS_DIR + '/instructors.csv')
            self._verify_instructors(df_inst_c)

            self.assertEqual(1, len(pipeline.classes_files))

            df_cls_j = pd.read_json(pipeline.classes_files[0] + '.json', lines=True)
            self._verify_classes(df_cls_j)

            df_cls_c = pd.read_csv(pipeline.classes_files[0] + '.csv')
            self._verify_classes(df_cls_c)

            return df_inst_j, df_inst_c, df_cls_j, df_cls_c
        df_inst_j, df_inst_c, df_cls_j, df_cls_c = _verify_exported_files()

        # check two iterations don't change anything
        pipeline = _run_spider([class_mock_item, wiki_mock_item])
        df_inst_j2, df_inst_c2, df_cls_j2, df_cls_c2 = _verify_exported_files()

        assert_frame_equal(df_inst_j, df_inst_j2)

    def _verify_instructors(self, df):
        self.assertEqual(2, len(df.index))
        self.assertEqual(1, len(df.loc[df['name'] == 'test instructor'].index))
        self.assertEqual(1, len(df.loc[df['wikipedia_link'].str
                                .contains('test_wikipedia', regex=False, na=False)].index))

    def _verify_classes(self, df):
        self.assertEqual(1, len(df.index))
        self.assertEqual(1, len(df.loc[df['course_code'] == 'test course_code'].index))
