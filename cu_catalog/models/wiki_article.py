from cu_catalog import config
from cu_catalog.models.text_classifier import TextClassifier
from cu_catalog.models.util import extract_word_stems2dict, words_match, words_match2


class WikiArticleClassifier(TextClassifier):
    LABEL_IRRELEVANT = 0
    LABEL_RELEVANT = 1

    def __init__(self):
        super(WikiArticleClassifier, self).__init__(config.DATA_WIKI_ARTICLE_TRAIN_FILENAME,
                                                    config.DATA_WIKI_ARTICLE_MODEL_FILENAME)

    @property
    def label_field_name(self):
        return 'label'

    def extract_features(self, row: dict) -> dict:
        features = {
            '__name_match_wiki_title': words_match(row['name'], row['wiki_title']),
            '__name_match_wiki_title2': words_match2(row['name'], row['wiki_title']),
            '__name_match_wiki_page': words_match(row['name'], row['wiki_page']),
            '__name_match_wiki_page2': words_match2(row['name'], row['wiki_page']),
            '__columbia_university_match': 'columbia university' in row['wiki_page'].lower(),
        }
        features.update(extract_word_stems2dict(row['wiki_page']))

        return features

    def eval_results(self, y_test, y_pred, show_predictions=True):
        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 1 and t != p]
        if len(mc):
            print("  Misclassified as irrelevant but are relevant:", mc)

        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 0 and t != p]
        if len(mc):
            print("  Misclassified as relevant but are not:", mc)
        print()

        super(WikiArticleClassifier, self).eval_results(y_test, y_pred, show_predictions)
