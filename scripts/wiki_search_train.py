# Classify wiki search results for which are to match / not-match or needs checking the article text.

import nltk
from cu_catalog.models.wiki_search import WikiSearchClassifier

if __name__ == '__main__':
    nltk.download('punkt')
    model = WikiSearchClassifier()
    model.load_training_data()
    model.fit()
    model.evaluate()
    model.persist_model()
