import json
import logging
import urllib
from scrapy import Request
from w3lib.html import remove_tags

import columbia_crawler.spiders.catalog as catalog
from columbia_crawler.items import WikipediaInstructorSearchResults, WikipediaInstructorPotentialArticle, \
    WikipediaInstructorArticle
from columbia_crawler.spiders.catalog_base import CatalogBase

logger = logging.getLogger(__name__)


# Functions related to searching instructors wikipedia profiles
class WikiSearch(CatalogBase):

    def _follow_search_wikipedia_instructor(self, instructor, class_listing):
        url = 'https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=' \
              + urllib.parse.quote_plus("Columbia University intitle:" + instructor)
        return Request(url, callback=self.parse_wiki_instructor_search_results,
                       meta={'class_listing': class_listing})

    def parse_wiki_instructor_search_results(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=Columbia+University+intitle%3ATanya+Zelevinsky
        @returns items 1 1
        @returns requests 0 0
        """
        cls = self._get_class_listing(response)
        instructor = cls['instructor']
        json_response = json.loads(response.body_as_unicode())
        search = json_response['query']['search']
        logger.info('WIKI: Search results for %s : %s', instructor, search)

        if len(search) == 0:
            return

        possible_match = []

        # yield search result item
        sr = WikipediaInstructorSearchResults()
        sr['name'] = instructor
        sr['class_listing'] = cls
        sr['search_results'] = search
        yield sr

        for result in search:
            title = result['title']
            snippet = result['snippet']

            if not catalog.CatalogSpider.validate_name(instructor, title):
                continue

            if len(search) == 1:
                yield WikipediaInstructorArticle(
                    name=instructor,
                    wikipedia_title=response.meta.get('wiki_title'))
                return

            snippet = remove_tags(snippet).upper()
            if "COLUMBIA UNIVERSITY" not in snippet:
                possible_match.append(result)
                continue

        # follow some possible articles and try to understand if it's related to searched instructors
        for result in possible_match:
            url = "https://en.wikipedia.org/wiki/" + urllib.parse.quote_plus(result['title'].replace(' ', '_'))
            yield Request(url, callback=self.parse_wiki_article_prof,
                          meta={**response.meta,
                                'instructor': instructor,
                                'wiki_title': result['title']})

        logger.info('WIKI: Not found obvious wiki search results for %s. Following articles: %s',
                    instructor, [x['title'] for x in possible_match])
        return

    @staticmethod
    def validate_name(instructor, title):
        for name_part in instructor.split(" "):
            if name_part not in title:
                return False
        return True

    # load the entire article from wikipedia
    def parse_wiki_article_prof(self, response):
        instructor = response.meta.get('instructor')
        page = remove_tags(response.body_as_unicode()).upper()

        yield WikipediaInstructorPotentialArticle(
            name=instructor,
            wikipedia_title=response.meta.get('wiki_title'),
            wikipedia_raw_page=response.body_as_unicode())

        if "COLUMBIA UNIVERSITY" in page:
            yield WikipediaInstructorArticle(
                name=instructor,
                wikipedia_title=response.meta.get('wiki_title'))
        else:
            logger.info("WIKI: Rejecting article '%s'. Not linked to professor: %s",
                        response.meta.get('wiki_title'), instructor)
