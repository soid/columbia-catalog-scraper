# Columbia University Directory of Classes Crawler

The project provides a set of data crawling scripts for .
The crawled data is available and updated in a separate repository [soid/columbia-catalog-data](https://github.com/soid/columbia-catalog-data).
 

## Use

Install Python3 and pip3, and required Python libraries:
```
pip3 install -r requirements.txt
```  

Train the models:
```
./run-script.sh scripts/wiki_search_train.py
./run-script.sh scripts/wiki_article_train.py
```

Run the crawler:
```
./run-crawler.sh -p
```

Run a sanity check for scrapped data:
```
./run-script.sh scripts/data_canary.py
```

## Development

Scrapy shell is very handy:

```
./scrapy-shell.sh 'https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=Michael+Collins+Columbia+University&utf8=&format=json'
...
>>> import json
>>> jsonresponse = json.loads(response.body_as_unicode())
>>> jsonresponse
>>> jsonresponse['query']
>>> import html
>>> from scrapy.selector import Selector
>>> s = Selector(text=html.unescape( jsonresponse['query']['search'][0]['snippet'] ))
>>> s.text()
```

Class parsing shell:
```
./scrapy-shell.sh 'http://www.columbia.edu//cu/bulletin/uwb/subj/COMS/E6998-20203-003/'

>>> from columbia_crawler.items import *
>>> parser = ColumbiaClassListing._Parser(response)
>>> parser._get_course_title()
'TOPICS IN COMPUTER SCIENCE'
```


## Common tasks

Label/Train Wikipedia search results for searching instructors' articles.
```
./run-script.sh scripts/wiki_search_label.py
./run-script.sh scripts/wiki_search_train.py
```

## Tests

```
./tests.sh
```

### Updating request data

For tests that use Betamax, remove appropriate file in cassettes/ folder and rerun the test.
