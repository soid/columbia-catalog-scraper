## Development

Scrapy shell is very handy:

```
scrapy shell 'https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=Michael+Collins+Columbia+University&utf8=&format=json'
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


## Common tasks

Label/Train Wikipedia search results for searching instructors' articles.
```
./run-script.sh scripts/wiki_search_label.py
./run-script.sh scripts/wiki_search_train.py
```

## Tests

### Updating request data

For tests that use Betamax, remove appropriate file in cassettes/ folder and rerun the test.
