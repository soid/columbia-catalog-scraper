import datetime
import json
import logging
import os
import random

import pandas as pd
import scrapy
from scrapy import Request
import urllib

from columbia_crawler.items import WikipediaInstructorSearchResults, WikipediaInstructorPotentialArticle, \
    WikipediaInstructorArticle
from cu_catalog import config
from cu_catalog.models.wiki_article import WikiArticleClassifier
from cu_catalog.models.wiki_search import WikiSearchClassifier, WSC

logger = logging.getLogger(__name__)


# Spider for searching instructors wikipedia profiles
class WikiSearch(scrapy.Spider):
    name = "wiki_search"

    custom_settings = {
        'HTTPCACHE_ENABLED': config.HTTP_CACHE_ENABLED,
        'ITEM_PIPELINES': {
            'columbia_crawler.pipelines.StoreWikiSearchResultsPipeline': 400,
        }
    }

    def __init__(self, *args, **kwargs):
        super(WikiSearch, self).__init__(*args, **kwargs)
        self.search_clf = WikiSearchClassifier()
        self.search_clf.load_model()
        self.article_clf = WikiArticleClassifier()
        self.article_clf.load_model()
        self.df_internal = None

    def start_requests(self):
        df = pd.read_json(config.DATA_INSTRUCTORS_JSON, lines=True)

        # join internal db to store last check and don't check too often
        if os.path.exists(config.DATA_INSTRUCTORS_INTERNAL_INFO_JSON):
            self.df_internal = pd.read_json(config.DATA_INSTRUCTORS_INTERNAL_INFO_JSON, lines=True)
        else:
            self.df_internal = pd.DataFrame(columns=['name', 'last_wikipedia_search'])

        df = df[df['wikipedia_link'].isnull()]
        df = df.join(self.df_internal.set_index('name'), on="name")
        df['last_wikipedia_search'] = pd.to_datetime(df['last_wikipedia_search'], unit='ms')

        # filter out recently checked instructors
        def recent_threshold(x):
            return x < datetime.datetime.now() - datetime.timedelta(days=random.randint(15, 45))
        df = df[df['last_wikipedia_search'].isna() | df['last_wikipedia_search'].apply(recent_threshold)]

        test_run = getattr(self, 'test_run', False)
        num = 0

        for index, row in df.iterrows():
            if row['name'] not in self.df_internal['name'].values:
                # create row
                self.df_internal = self.df_internal.append({'name': row['name']}, ignore_index=True)
            self.df_internal.loc[self.df_internal['name'] == row['name'],
                                 'last_wikipedia_search'] = datetime.datetime.now()

            yield self._follow_search_wikipedia_instructor(row['name'],
                                                           "; ".join(row['departments']))

            num += 1
            if test_run and num > 20:
                break

    def _follow_search_wikipedia_instructor(self, instructor: str, department: str):
        url = 'https://en.wikipedia.org/w/api.php?action=query&list=search&utf8=&format=json&srsearch=' \
              + urllib.parse.quote_plus("Columbia University intitle:" + instructor)
        return Request(url, callback=self.parse_wiki_instructor_search_results,
                       meta={'instructor': instructor,
                             'department': department})

    def close(self, reason):
        if self.df_internal:
            logger.info("Started updating internal db")
            os.makedirs(config.DATA_INTERNAL_DB_DIR, exist_ok=True)
            file_json = open(config.DATA_INSTRUCTORS_INTERNAL_INFO_JSON, 'w')
            self.df_internal.sort_values(by=['name'], inplace=True)
            self.df_internal.to_json(path_or_buf=file_json, orient="records", lines=True)
            file_json.close()
            logger.info("Finished updating internal db")

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

        json_response = json.loads(response.body_as_unicode())
        search = json_response['query']['search']
        logger.info('WIKI: Search results for %s : %s', instructor, search)

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
        if config.IN_TEST:
            instructor = "Test Testoff"
            department = "Memology"

        json_response = json.loads(response.body_as_unicode())
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
        rows = [self.article_clf.extract_features2vector(item.to_dict())]
        pred = self.article_clf.predict(rows)
        if pred == WikiArticleClassifier.LABEL_RELEVANT:
            yield WikipediaInstructorArticle(
                name=instructor,
                wikipedia_title=page['title'])
