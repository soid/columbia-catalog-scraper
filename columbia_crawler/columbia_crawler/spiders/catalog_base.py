from columbia_crawler import config
from columbia_crawler.items import ColumbiaDepartmentListing, ColumbiaClassListing


class CatalogBase:
    # helpers
    def _get_department_listing(self, response):
        return response.meta.get('department_listing',
                                 ColumbiaDepartmentListing(
                                     department_code="TEST",
                                     term_month="testing",
                                     term_year="testing",
                                     raw_content="test content")
                                 if config.IN_TEST else None)

    def _get_class_listing(self, response):
        return response.meta.get('class_listing',
                                 ColumbiaClassListing(
                                     department_listing=self._get_department_listing(response),
                                     instructor="testing instr",
                                     class_id="testing class",
                                     course_descr="test content")
                                 if config.IN_TEST else None)
