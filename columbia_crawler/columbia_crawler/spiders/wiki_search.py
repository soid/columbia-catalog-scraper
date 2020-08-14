import json
import logging
import urllib
from scrapy import Request

from columbia_crawler.items import WikipediaInstructorSearchResults, WikipediaInstructorPotentialArticle, \
    WikipediaInstructorArticle
from columbia_crawler.spiders.catalog_base import CatalogBase
from cu_catalog.models.wiki_article import WikiArticleClassifier
from cu_catalog.models.wiki_search import WikiSearchClassifier, WSC

logger = logging.getLogger(__name__)


# Functions related to searching instructors wikipedia profiles
class WikiSearch(CatalogBase):

    initialized = False  # because scrapy contracts won't call __init__

    def init(self):
        # this function because Scrapy __init__ in not called for this class for some reason
        if not WikiSearch.initialized:
            self.search_clf = WikiSearchClassifier()
            self.search_clf.load_model()
            self.article_clf = WikiArticleClassifier()
            self.article_clf.load_model()
            WikiSearch.initialized = True

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
        self.init()

        cls = self._get_class_listing(response)
        instructor = cls['instructor']
        json_response = json.loads(response.body_as_unicode())
        search = json_response['query']['search']
        logger.info('WIKI: Search results for %s : %s', instructor, search)

        if len(search) == 0:
            return

        # yield search result item
        sr = WikipediaInstructorSearchResults()
        sr['name'] = instructor
        sr['class_listing'] = cls
        sr['search_results'] = search
        yield sr

        # use classifier to find match or possible match
        rows = []
        rows_vec = []
        for result in search:
            title = result['title']
            snippet = result['snippet']
            row = {
                'name': instructor,
                'department': cls['department'],
                'search_results.title': title,
                'search_results.snippet': snippet,
            }
            rows.append(row)
            rows_vec.append(self.search_clf.extract_features2vector(row))

        # see if we found worthy items
        pred = self.search_clf.predict(rows_vec)
        for p, row in zip(pred, rows):
            if p == WSC.LABEL_RELEVANT:
                yield WikipediaInstructorArticle(
                    name=instructor,
                    wikipedia_title=row['search_results.title'])
                break
            if p == WSC.LABEL_POSSIBLY:
                url = 'https://en.wikipedia.org/w/api.php?' \
                      'format=json&action=query&prop=extracts&exlimit=max&' \
                      'explaintext&titles='\
                      + urllib.parse.quote_plus(row['search_results.title']) \
                      + '&redirects='
                yield Request(url, callback=self.parse_wiki_article_prof,
                              meta={**response.meta,
                                    'instructor': instructor,
                                    'class_listing': cls,
                                    'wiki_title': row['search_results.title']})

    # load the entire article from wikipedia
    def parse_wiki_article_prof(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&titles=Caroline+Pafford+Miller&redirects=> (referer: https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=Columbia+University+intitle%3ACaroline+Miller
        @returns items 1 1
        @returns requests 0 0
        """
        self.init()

        cls = self._get_class_listing(response)
        # instructor = response.meta.get('instructor')
        instructor = cls['instructor']

        json_response = json.loads(response.body_as_unicode())
        pages = json_response['query']['pages']
        page = next(iter(pages.values()))

        item = WikipediaInstructorPotentialArticle(
            name=instructor,
            class_listing=cls,
            wikipedia_title=page['title'],
            wikipedia_raw_page=page['extract'])
        yield item

        rows = [self.article_clf.extract_features2vector(item.to_json())]
        pred = self.article_clf.predict(rows)
        if pred == WikiArticleClassifier.LABEL_RELEVANT:
            yield WikipediaInstructorArticle(
                name=instructor,
                wikipedia_title=page['title'])
