from unittest import TestCase

from columbia_crawler.items import ColumbiaClassListing, WikipediaInstructorArticle
from columbia_crawler.pipelines import StoreClassPipeline
from columbia_crawler.spiders.catalog import CatalogSpider


class TestStoreClassPipeline(TestCase):
    def test(self):
        spider = CatalogSpider()
        scp = StoreClassPipeline()
        scp.open_spider(spider)

        mock_item = ColumbiaClassListing.get_test()
        scp.process_item(mock_item, spider)

        mock_item = WikipediaInstructorArticle.get_test()
        scp.process_item(mock_item, spider)

        scp.close_spider(spider)
