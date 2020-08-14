from abc import ABCMeta, abstractmethod
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split

from cu_catalog.models.util import Dict2Vect

import joblib


class TextClassifier(metaclass=ABCMeta):
    def __init__(self, data_filename: str, model_filename: str):
        self.data_filename = data_filename
        self.model_filename = model_filename

        self.dict2vect: Dict2Vect or None = None
        self.clf: RandomForestClassifier or None = None

        # fields for training only
        self.data: pd.DataFrame or None = None
        [self.X_all, self.y_all,
         self.X_train, self.X_test,
         self.y_train, self.y_test] = [None] * 6

    def _load_data(self):
        self.data = pd.read_json(self.data_filename, lines=True)

    @property
    @abstractmethod
    def label_field_name(self):
        raise NotImplementedError

    @abstractmethod
    def extract_features(self, row):
        raise NotImplementedError

    def custom_decider(self, scores: list) -> int:
        return np.argmax(scores)

    def balance(self, df: pd.DataFrame):
        return df

    def extract_features2vector(self, row: dict) -> list:
        return self.dict2vect.convert(self.extract_features(row))

    def data_transform(self, data, test_size=0.2):
        # extract features
        X = data.apply(lambda x: self.extract_features(x.to_dict()), axis=1).values
        # transform features to vectors
        self.dict2vect = Dict2Vect(X)
        X = np.array(list(map(self.dict2vect.convert, X)))
        Y = data[self.label_field_name]

        # train/test split
        return [X, Y] + train_test_split(X, Y, test_size=test_size, stratify=Y, random_state=41)

    def view_sample(self, idx):
        sample = self.data.loc[idx].to_dict()
        print(sample)
        print(self.extract_features(sample))
        display(self.data[idx:idx+1])

    @staticmethod
    def show_true_pred(y_true, y_pred):
        """View predicted values"""
        print("Predicted:")
        print(np.array(y_pred))
        print("True:")
        print(y_true.values)

    def load_training_data(self):
        self._load_data()
        data_balanced = self.balance(self.data)
        self.X_all, self.y_all, self.X_train, self.X_test, self.y_train, self.y_test \
            = self.data_transform(data_balanced)
        print("Initial data for training before balancing:")
        display(self.data[self.label_field_name].value_counts())
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
        self.eval_results(self.y_test, y_pred)
        print()

        print("Evaluating the entire unbalanced data set:")
        X = self.data.apply(lambda x: self.extract_features2vector(x.to_dict()), axis=1)
        X = list(X)
        y = self.data[self.label_field_name]

        y_pred = self.predict(X)
        self.eval_results(y, y_pred, show_predictions=False)
        self.show_confusion_matrix(y, y_pred)

    def eval_results(self, y_test, y_pred, show_predictions=True):
        print(classification_report(y_test, y_pred))
        if show_predictions:
            self.show_true_pred(y_test, y_pred)

    def show_confusion_matrix(self, y_test, y_pred):
        print("Confusion Matrix:")
        print(confusion_matrix(y_test, y_pred))

    def predict(self, rows):
        y_scores = self.clf.predict_proba(rows)
        return [self.custom_decider(y) for y in y_scores]

    def persist_model(self):
        model = {
            'clf': self.clf,
            'dict2vec': self.dict2vect
        }
        joblib.dump(model, self.model_filename)

    def load_model(self):
        model = joblib.load(self.model_filename)
        self.clf = model['clf']
        self.dict2vect = model['dict2vec']


if 'display' not in vars():
    display = print
