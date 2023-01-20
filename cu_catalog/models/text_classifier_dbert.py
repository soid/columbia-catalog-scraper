from abc import ABCMeta, abstractmethod

import numpy as np
import torch
from datasets import load_dataset
from sklearn.metrics import accuracy_score, f1_score
from sklearn.metrics import classification_report, confusion_matrix
from scipy.special import softmax
from transformers import AutoModelForSequenceClassification, AutoTokenizer, TrainingArguments, Trainer
from cu_catalog import config


# DistilBERT based text classifier
class TextClassifierDBERT(metaclass=ABCMeta):
    def __init__(self, data_filename: str, model_filename: str):
        self.data_filename = data_filename
        self.model_filename = model_filename
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def _load_data(self):
        self.data = load_dataset("json", data_files=self.data_filename, split='train')
        self.data.set_format('pandas')
        self.dataset_train = load_dataset("json", data_files=self.data_filename, split='train[:70%]')
        self.dataset_test = load_dataset("json", data_files=self.data_filename, split='train[70%:]')

    @property
    @abstractmethod
    def label_field_name(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def label_class_weights(self):
        raise NotImplementedError

    @property
    def training_params(self):
        """
        :return: epochs, batch_size
        """
        return 10, 2

    @abstractmethod
    def tokenize(self, batch, return_tensors=None):
        raise NotImplementedError

    def view_sample(self, idx):
        sample = self.data[idx].to_dict()
        # print(sample)
        display(sample)
        # display(self.data[idx])

    @staticmethod
    def show_true_pred(y_true, y_pred):
        """View predicted values"""
        print("Predicted:")
        print(np.array(y_pred))
        print("True:")
        print(np.array(y_true))

    def load_training_data(self):
        self._load_data()
        print("Initial all data:")
        self.data.set_format('pandas')
        display(self.data[self.label_field_name].value_counts())
        self.data.set_format(None)
        print()

        print("Train set:")
        self.dataset_train.set_format('pandas')
        display(self.dataset_train[self.label_field_name].value_counts())
        self.dataset_train.set_format(None)
        print()

        print("Test set:")
        self.dataset_test.set_format('pandas')
        display(self.dataset_test[self.label_field_name].value_counts())
        self.dataset_test.set_format(None)


    def fit(self):
        model_ckpt = "distilbert-base-uncased"
        num_labels = len(self.label_class_weights)
        self.model = AutoModelForSequenceClassification \
            .from_pretrained(model_ckpt, num_labels=num_labels) \
            .to(self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(model_ckpt)

        encoded_train = self.dataset_train.map(self.tokenize)
        encoded_test = self.dataset_test.map(self.tokenize)

        epochs, batch_size = self.training_params

        device = self.device
        weights = self.label_class_weights
        class CustomTrainer(Trainer):
            def compute_loss(self, model, inputs, return_outputs=False):
                labels = inputs.get("labels")
                # forward pass
                outputs = model(**inputs)
                logits = outputs.get("logits")
                # compute custom loss (suppose one has 3 labels with different weights)
                loss_fct = torch.nn.CrossEntropyLoss(weight=torch.tensor(weights).to(device))
                loss = loss_fct(logits.view(-1, self.model.config.num_labels), labels.view(-1))
                return (loss, outputs) if return_outputs else loss

        def compute_metrics(pred):
            labels = pred.label_ids
            preds = pred.predictions.argmax(-1)
            f1 = f1_score(labels, preds, average="weighted")
            acc = accuracy_score(labels, preds)
            return {"accuracy": acc, "f1": f1}

        training_args = TrainingArguments(
            output_dir=config.DATA_WIKI_ARTICLE_MODEL_CHECKPOINTS,  # TODO customize?
            evaluation_strategy="epoch",
            save_strategy="epoch",
            learning_rate=2e-5,
            per_device_train_batch_size=batch_size,
            per_device_eval_batch_size=batch_size,
            num_train_epochs=epochs,
            weight_decay=0.01,
            disable_tqdm=False,
            logging_steps=batch_size,
            load_best_model_at_end=True
        )

        self.trainer = CustomTrainer(
            model=self.model,
            args=training_args,
            compute_metrics=compute_metrics,
            train_dataset=encoded_train,
            eval_dataset=encoded_test,
            tokenizer=self.tokenizer,
        )

        self.trainer.train()

    def evaluate(self, dataset=None):
        if dataset is None:
            dataset = self.dataset_test
        print("Evaluating test data set:")
        self.data.set_format(None)
        y_pred = self.predict(dataset)
        self.data.set_format('pandas')

        y_test = dataset[self.label_field_name]
        self.eval_results(y_test, y_pred)
        print()
        self.show_confusion_matrix(y_test, y_pred)

    def eval_results(self, y_test, y_pred, show_predictions=True):
        print(classification_report(y_test, y_pred))
        if show_predictions:
            self.show_true_pred(y_test, y_pred)

    def show_confusion_matrix(self, y_test, y_pred):
        print("Confusion Matrix:")
        print(confusion_matrix(y_test, y_pred))

    def predict_proba(self, rows):
        predicted = []
        for row in rows:
            inputs = self.tokenize(row, return_tensors='pt')
            inputs = inputs.to(self.device)
            logits = self.model(**inputs)
            logits = logits.logits.cpu().detach().numpy()
            proba = softmax(logits, axis=1)
            predicted.append(proba[0])
        return np.array(predicted)

    def predict(self, rows):
        proba = self.predict_proba(rows)
        predicted_labels = proba.argmax(-1)
        return predicted_labels

    def persist_model(self):
        self.trainer.save_model(self.model_filename)

    def load_model(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_filename, local_files_only=True)
        self.model = AutoModelForSequenceClassification \
            .from_pretrained(self.model_filename, local_files_only=True) \
            .to(self.device)


if 'display' not in vars():
    display = print
