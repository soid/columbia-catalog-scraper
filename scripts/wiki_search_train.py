# Classify wiki search results for which are to match / not-match or needs checking the article text.

from models.wiki_search import WikiSearchClassifier

if __name__ == '__main__':
    model = WikiSearchClassifier()
    model.load_training_data()
    model.fit()
    model.evaluate()
    model.persist_model()
