from cu_catalog.models.wiki_article import WikiArticleClassifier

if __name__ == '__main__':
    model = WikiArticleClassifier()
    model.load_training_data()
    model.fit()
    model.evaluate()
    model.persist_model()
