import pandas as pd

from sklearn.metrics import confusion_matrix

from datasets import Dataset
from cu_catalog import config
from cu_catalog.models.text_classifier_dbert import TextClassifierDBERT


class WikiSearchClassifier(TextClassifierDBERT):
    LABEL_IRRELEVANT = 0
    LABEL_POSSIBLY = 1
    LABEL_RELEVANT = 2

    def __init__(self):
        super(WikiSearchClassifier, self).__init__(config.DATA_WIKI_SEARCH_TRAIN_FILENAME,
                                                   config.DATA_WIKI_SEARCH_MODEL_FILENAME)
        self.threshold = 0.6

    @property
    def label_field_name(self):
        return 'search_results.label'

    @property
    def label_class_weights(self):
        return [1.0, 13.0, 25.0]

    @property
    def training_params(self):
        return 30, 32

    def tokenize(self, batch, return_tensors=None):
        inputs = batch['name'] \
                 + ' from departments of' + batch['department'] \
                 + '[SEP]' + batch['search_results.title'] \
                 + '[SEP]' + batch['search_results.snippet']
        return self.tokenizer(inputs, padding=True, truncation=True, return_tensors=return_tensors)

    def _load_data(self):
        super(WikiSearchClassifier, self)._load_data()
        def transf(data):
            dataf = data
            data.set_format('pandas')
            data = data[:].T.to_dict().values()
            for x in data:
                x['search_results'] = list(x['search_results'])
            data = pd.json_normalize(data,
                                     'search_results', ['name', 'department'],
                                     record_prefix='search_results.')
            data['labels'] = data['search_results.label']
            dataf.set_format(None)
            return Dataset.from_pandas(data)
        self.data = transf(self.data)
        self.dataset_train = transf(self.dataset_train)
        self.dataset_test = transf(self.dataset_test)


    def show_confusion_matrix(self, y_test, y_pred):
        print("Confusion Matrix:")
        labels = ['Irrelevant', 'Possible', 'Relevant']
        print(pd.DataFrame(confusion_matrix(y_test, y_pred),
                           columns=labels, index=labels))

    def eval_results(self, y_test, y_pred, show_predictions=True):
        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 2 and p == 0]
        print("  Misclassified as irrelevant but are relevant (no big deal):", mc)

        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if p == 2 and t != p]
        print("  Misclassified as relevant but are not (big deal, unless mistake in dataset):", mc)
        print()

        super(WikiSearchClassifier, self).eval_results(y_test, y_pred, show_predictions)


# Various helpers

WSC = WikiSearchClassifier  # Just shortcut
