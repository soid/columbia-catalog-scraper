from cu_catalog import config
from cu_catalog.models.text_classifier_dbert import TextClassifierDBERT


class WikiArticleClassifier(TextClassifierDBERT):
    LABEL_IRRELEVANT = 0
    LABEL_RELEVANT = 1

    def __init__(self):
        super(WikiArticleClassifier, self).__init__(config.DATA_WIKI_ARTICLE_TRAIN_FILENAME,
                                                    config.DATA_WIKI_ARTICLE_MODEL_FILENAME)

    @property
    def label_field_name(self):
        return 'label'

    @property
    def label_class_weights(self):
        return [1.0, 2.0]

    @property
    def training_params(self):
        return 30, 4

    def tokenize(self, batch, return_tensors=None):
        inputs = batch['name'] + ' is ' + batch['wiki_title']\
                 + ', who teaches ' + batch['department'] \
                 + '[SEP]' + batch['wiki_title'] \
                 + "[SEP]" + batch['wiki_page']
        inputs = 'Is ' + batch['name'] + ' the same as ' + batch['wiki_title'] \
                 + ', who teaches ' + batch['department'] + "?" \
                 + "[SEP]"  + batch['wiki_page']
        inputs = batch['name'] \
                 + ' [SEP] ' + batch['department'] \
                 + ' [SEP] ' + batch['wiki_title'] \
                 + " [SEP] " + batch['wiki_page']
        return self.tokenizer(inputs, padding=True, truncation=True, return_tensors=return_tensors)

    def eval_results(self, y_test, y_pred, show_predictions=True):
        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 1 and t != p]
        if len(mc):
            print("  Misclassified as irrelevant but are relevant:", mc)

        mc = [i for i, (t, p) in enumerate(zip(y_test, y_pred)) if t == 0 and t != p]
        if len(mc):
            print("  Misclassified as relevant but are not:", mc)
        print()

        super(WikiArticleClassifier, self).eval_results(y_test, y_pred, show_predictions)
