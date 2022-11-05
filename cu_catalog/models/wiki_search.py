import pandas as pd

from sklearn.metrics import classification_report, confusion_matrix

from nltk.tokenize import word_tokenize

from cu_catalog import config
from cu_catalog.models.text_classifier import TextClassifier
from cu_catalog.models.util import words_match, words_match2, str_similarity, extract_word_stems2dict


class WikiSearchClassifier(TextClassifier):
    LABEL_IRRELEVANT = 0
    LABEL_POSSIBLY = 1
    LABEL_RELEVANT = 2

    def __init__(self):
        super(WikiSearchClassifier, self).__init__(config.DATA_WIKI_SEARCH_TRAIN_FILENAME,
                                                   config.DATA_WIKI_SEARCH_MODEL_FILENAME)

    @property
    def label_field_name(self):
        return 'search_results.label'

    def _load_data(self):
        super(WikiSearchClassifier, self)._load_data()
        self.data = pd.json_normalize(self.data.T.to_dict().values(),
                                      'search_results', ['name', 'department'], record_prefix='search_results.')

    def custom_decider(self, scores: list) -> int:
        """ This decider is based on calculations from precision/recall trade-off in Jupyter.

        We want:
            - high precision for LABEL_RELEVANT
            - high recall for LABEL_POSSIBLY
            - don't care much about LABEL_IRRELEVANT.
        Overall, it's better to mistakenly classify as LABEL_POSSIBLY
        and defer the decision for the wiki article classifier
        than to mis-classify RELEVANT/IRRELEVANT labels.
        """
        if scores[WSC.LABEL_RELEVANT] > 0.75:
            return WSC.LABEL_RELEVANT
        if scores[WSC.LABEL_POSSIBLY] > 0.3:
            return WSC.LABEL_POSSIBLY
        if scores[WSC.LABEL_RELEVANT] > 0.47:
            return WSC.LABEL_POSSIBLY

        return WSC.LABEL_IRRELEVANT

    def extract_features(self, row: dict) -> dict:
        features = {'__name_match_title': words_match(row['name'], row['search_results.title']),
                    '__name_match_title2': words_match2(row['name'], row['search_results.title']),
                    '__name_similarity_title': str_similarity(row['name'], row['search_results.title']),
                    '__name_match_snippet': words_match(row['name'], row['search_results.snippet']),
                    '__name_match_snippet2': words_match2(row['name'], row['search_results.snippet']),
                    '__department_similarity_snippet': str_similarity(row['department'],
                                                                      row['search_results.snippet']),
                    '__columbia_match_snippet': words_match('columbia', row['search_results.snippet'])}

        features.update(extract_word_stems2dict(row['search_results.snippet']))

        return features

    def show_confusion_matrix(self, y_test, y_pred):
        print("Confusion Matrix:")
        labels = ['Irrelevant', 'Possible', 'Relevant']
        print(pd.DataFrame(confusion_matrix(y_test, y_pred),
                           columns=labels, index=labels))

    def eval_results(self, y_test, y_pred, show_predictions=True):
        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 2 and t != p]
        if len(mc):
            print("  Misclassified as irrelevant but are relevant (no big deal):", mc)

        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if p == 2 and t != p]
        if len(mc):
            print("  Misclassified as relevant but are not (big deal, unless mistake in dataset):", mc)
        print()

        super(WikiSearchClassifier, self).eval_results(y_test, y_pred, show_predictions)

    # def balance(self, df: pd.DataFrame):
    #     # balance all labels
    #     g = df.groupby('search_results.label')
    #     return g.apply(lambda x: x.sample(g.size().min(), random_state=42).reset_index(drop=True))


# Various helpers

WSC = WikiSearchClassifier  # Just shortcut
