from typing import Any

import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix

from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

import textdistance
import joblib

from columbia_crawler import config


class WikiSearchClassifier:
    LABEL_IRRELEVANT = 0
    LABEL_POSSIBLY = 1
    LABEL_RELEVANT = 2

    def __init__(self):
        self.dict2vect: Dict2Vect or None = None
        self.clf: RandomForestClassifier or None = None

        # fields for training only
        self.data: pd.DataFrame or None = None
        self.porter = PorterStemmer()
        [self.X_all, self.y_all,
         self.X_train, self.X_test,
         self.y_train, self.y_test] = [None] * 6

    def _load_data(self):
        data = pd.read_json(config.DATA_WIKI_SEARCH_TRAIN_FILENAME, lines=True)
        self.data = pd.json_normalize(data.T.to_dict().values(),
                                      'search_results', ['name', 'department'], record_prefix='search_results.')

    @staticmethod
    def custom_decider(scores: list) -> int:
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
                    '__name_similarity_title': words_match_partial(row['name'], row['search_results.title']),
                    '__name_match_snippet': words_match(row['name'], row['search_results.snippet']),
                    '__name_match_snippet2': words_match2(row['name'], row['search_results.snippet']),
                    '__department_similarity_snippet': words_match_partial(row['department'],
                                                                           row['search_results.snippet']),
                    '__columbia_match_snippet': words_match('columbia', row['search_results.snippet'])}

        # extract stems for every word
        tokens = word_tokenize(row['search_results.snippet'])
        for t in tokens:
            features["__word_" + self.porter.stem(t)] = True

        return features

    def extract_features2vector(self, row: dict) -> list:
        return self.dict2vect.convert(self.extract_features(row))

    def data_transform(self, data, test_size=0.2):
        # extract features
        X = data.apply(lambda x: self.extract_features(x.to_dict()), axis=1).values
        # transform features to vectors
        self.dict2vect = Dict2Vect(X)
        X = np.array(list(map(self.dict2vect.convert, X)))
        Y = data['search_results.label']

        # train/test split
        return [X, Y] + train_test_split(X, Y, test_size=test_size, stratify=Y, random_state=42)

    # View predicted values
    @staticmethod
    def show_true_pred(y_true, y_pred):
        print("Predicted:")
        print(np.array(y_pred))
        print("True:")
        print(y_true.values)

    @staticmethod
    def show_confusion_matrix(y_test, y_pred):
        print("Confusion Matrix:")
        print(pd.DataFrame(confusion_matrix(y_test, y_pred),
                           columns=['Irrelevant', 'Possible', 'Relevant'], index=['Irrelevant', 'Possible', 'Relevant']))

    @staticmethod
    def eval_results(y_test, y_pred, show_predictions=True):
        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 2 and t != p]
        if len(mc):
            print("  Misclassified as irrelevant but are relevant (no big deal):", mc)

        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if p == 2 and t != p]
        if len(mc):
            print("  Misclassified as relevant but are not (big deal, unless mistake in dataset):", mc)
        print()

        print(classification_report(y_test, y_pred))
        if show_predictions:
            WSC.show_true_pred(y_test, y_pred)
            # display(y_test)

    @staticmethod
    def balance(df: pd.DataFrame):
        # balance all labels
        g = df.groupby('search_results.label')
        return g.apply(lambda x: x.sample(g.size().min(), random_state=42).reset_index(drop=True))

    def view_sample(self, idx):
        sample = self.data.loc[idx].to_dict()
        print(sample)
        print(self.extract_features(sample))
        display(self.data[idx:idx+1])

    def load_training_data(self):
        self._load_data()
        data_balanced = WikiSearchClassifier.balance(self.data)
        self.X_all, self.y_all, self.X_train, self.X_test, self.y_train, self.y_test \
            = self.data_transform(data_balanced)
        print("Initial data for training before balancing:")
        display(self.data['search_results.label'].value_counts())
        print("Train set:")
        display(self.y_train.value_counts())
        print("Test set:")
        display(self.y_test.value_counts())

    def fit(self):
        self.clf = RandomForestClassifier()
        self.clf.fit(self.X_train, self.y_train)

    def evaluate(self):
        print("Evaluating test data set:")
        y_pred = self.predict(self.X_test)
        WSC.eval_results(self.y_test, y_pred)
        print()

        print("Evaluating the entire unbalanced data set:")
        X = self.data.apply(lambda x: self.extract_features2vector(x.to_dict()), axis=1)
        X = list(X)
        y = self.data['search_results.label']

        y_pred = self.predict(X)
        WSC.eval_results(y, y_pred, show_predictions=False)
        WSC.show_confusion_matrix(y, y_pred)

    def predict(self, rows):
        y_scores = self.clf.predict_proba(rows)
        return [WSC.custom_decider(y) for y in y_scores]

    def persist_model(self):
        model = {
            'clf': self.clf,
            'dict2vec': self.dict2vect
        }
        joblib.dump(model, config.DATA_WIKI_SEARCH_MODEL_FILENAME)

    def load_model(self):
        model = joblib.load(config.DATA_WIKI_SEARCH_MODEL_FILENAME)
        self.clf = model['clf']
        self.dict2vect = model['dict2vec']


# Various helpers

WSC = WikiSearchClassifier  # Just shortcut


class Dict2Vect:
    """ Convert a list of dictionaries into a list of lists (vectors)
    where each index represents certain key from the dictionary

    >>> d2v = Dict2Vect([{'a': 1, 'b': 2}, {'c': 9, 'd': 99}], absent_value=0)
    >>> d2v.convert({'a': 3})
    [3, 0, 0, 0]
    >>> d2v.convert({'c': 2, 'z': 88})
    [0, 0, 2, 0]
    """
    def __init__(self, data, absent_value: Any = False):
        self.keys = list({k for x in data for k in x.keys()})
        self.keys.sort()
        self.absent_value = absent_value

    def convert(self, row: dict):
        return [row[k] if k in row else self.absent_value for k in self.keys]


def words_match(instructor, title):
    title = title.lower()
    instructor = instructor.lower()
    for name_part in instructor.split():
        if name_part not in title:
            return False
    return True


def words_match2(instructor, title):  # skip short names
    title = title.lower()
    instructor = instructor.lower()
    for name_part in instructor.split():
        if len(name_part) == 1:
            continue
        if name_part not in title:
            return False
    return True


def words_match_partial(str1, str2):
    str1, str2 = str1.lower(), str2.lower()
    return textdistance.jaro_winkler.normalized_similarity(str1, str2)


if 'display' not in vars():
    display = print
