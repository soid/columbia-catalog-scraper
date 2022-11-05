import datetime
import json
import logging
import random

import pandas as pd
import scrapy
from scrapy import Request
import urllib

from columbia_crawler import util
from columbia_crawler.items import WikipediaInstructorSearchResults, WikipediaInstructorPotentialArticle, \
    WikipediaInstructorArticle
from cu_catalog import config
from cu_catalog.models.wiki_article import WikiArticleClassifier
from cu_catalog.models.wiki_search import WikiSearchClassifier, WSC

logger = logging.getLogger(__name__)


# Spider for searching instructors wikipedia profiles
class WikiSearchSpider(scrapy.Spider):
    name = "wiki_search"

    custom_settings = {
        'HTTPCACHE_ENABLED': config.HTTP_CACHE_ENABLED,
        'HTTPCACHE_DIR': config.HTTPCACHE_DIR,
        'LOG_LEVEL': config.LOG_LEVEL,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreWikiSearchResultsPipeline': 400,
        }
    }

    def __init__(self, *args, **kwargs):
        super(WikiSearchSpider, self).__init__(*args, **kwargs)
        self.search_clf = WikiSearchClassifier()
        self.search_clf.load_model()
        self.article_clf = WikiArticleClassifier()
        self.article_clf.load_model()
        self.instructors_internal_db = None

    def start_requests(self):
        self.crawler.stats.set_value('wiki_articles_loaded', 0)
        self.crawler.stats.set_value('wiki_searches', 0)

        yield self._follow_search_wikipedia_instructor("Brian Greene",
              "Department: Physics, Summer Session (SUMM), School of Professional Studies (SPS)")
        return

        df = pd.read_json(config.DATA_INSTRUCTORS_JSON)

        # join internal db to store last check and don't check too often
        self.instructors_internal_db = util.InstructorsInternalDb(['last_wikipedia_search'])

        df = df[df['wikipedia_link'].isnull()]
        df = df.join(self.instructors_internal_db.df_internal.set_index('name'), on="name")
        df['last_wikipedia_search'] = pd.to_datetime(df['last_wikipedia_search'], unit='ms')

        # filter out recently checked instructors
        def recent_threshold(x):
            return x < datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 1))
        df = df[df['last_wikipedia_search'].isna() | df['last_wikipedia_search'].apply(recent_threshold)]

        def _yield(x):
            _, row = x
            self.instructors_internal_db.update_instructor(row['name'], 'last_wikipedia_search')
            return self._follow_search_wikipedia_instructor(row['name'],
                                                            "; ".join(row['departments']))
        yield from util.spider_run_loop(self, df.iterrows(), _yield)


    def _follow_search_wikipedia_instructor(self, instructor: str, department: str):
        self.crawler.stats.inc_value('wiki_searches')
        url = 'https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=' \
              + urllib.parse.quote_plus("Columbia University intitle:" + instructor)
        return Request(url, callback=self.parse_wiki_instructor_search_results,
                       meta={'instructor': instructor,
                             'department': department})

    def close(self, reason):
        if self.instructors_internal_db is not None:
            self.instructors_internal_db.store()

    def parse_wiki_instructor_search_results(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=Columbia+University+intitle%3ATanya+Zelevinsky
        @returns items 1 1
        @returns requests 0 0
        """
        instructor = response.meta.get('instructor')
        department = response.meta.get('department')
        if config.IN_TEST:
            instructor = "Test Testoff"
            department = "Memology"

        json_response = json.loads(response.text)
        search = json_response['query']['search']
        logger.debug('WIKI: Search results for %s : %s', instructor, search)

        if len(search) == 0:
            return

        # yield search result item
        sr = WikipediaInstructorSearchResults()
        sr['name'] = instructor
        sr['department'] = department
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
                'department': department,
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
                    department=department,
                    wikipedia_title=row['search_results.title'])
                break
            if p == WSC.LABEL_POSSIBLY:
                self.crawler.stats.inc_value('wiki_articles_loaded')
                url = 'https://en.wikipedia.org/w/api.php?' \
                      'format=json&action=query&prop=extracts&exlimit=max&' \
                      'explaintext&titles='\
                      + urllib.parse.quote_plus(row['search_results.title']) \
                      + '&redirects='
                yield Request(url, callback=self.parse_wiki_article_prof,
                              meta={**response.meta,
                                    'instructor': instructor,
                                    'department': department,
                                    'wiki_title': row['search_results.title']})

    # load the entire article from wikipedia
    def parse_wiki_article_prof(self, response):
        """ Starting with department list, crawl all listings by each department.

        @url https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&titles=Caroline+Pafford+Miller&redirects=> (referer: https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=Columbia+University+intitle%3ACaroline+Miller
        @returns items 1 1
        @returns requests 0 0
        """
        instructor = response.meta.get('instructor')
        department = response.meta.get('department')
        if config.IN_TEST and not instructor:
            instructor = "Test Testoff"
            department = "Memology"

        json_response = json.loads(response.text)
        pages = json_response['query']['pages']
        page = next(iter(pages.values()))

        # saving potential articles is useful for training the classifier later
        item = WikipediaInstructorPotentialArticle(
            name=instructor,
            department=department,
            wikipedia_title=page['title'],
            wikipedia_raw_page=page['extract'])
        yield item

        # predict/classify if article is related to instructor
        rows = [item.to_dict()]
        pred = self.article_clf.predict(rows)
        if pred[0] == WikiArticleClassifier.LABEL_RELEVANT:
            yield WikipediaInstructorArticle(
                name=instructor,
                department=department,
                wikipedia_title=page['title'])
