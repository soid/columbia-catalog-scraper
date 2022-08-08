import json
import os
from collections import defaultdict
from typing import List

import numpy as np
import pandas as pd

import cu_catalog.config as config


def load_instructors():
    instructors = defaultdict(lambda: {}, {})
    if os.path.exists(config.DATA_INSTRUCTORS_JSON):
        with open(config.DATA_INSTRUCTORS_JSON, 'r') as f:
            for line in f:
                instr = json.loads(line)
                instructors[instr['name']] = instr
                if instr['departments']:
                    instr['departments'] = set(instr['departments'])
    return instructors


def load_term(term):
    filename = config.DATA_CLASSES_DIR + '/' + term + '.json'
    if not os.path.exists(filename):
        return
    df = pd.read_json(filename, dtype=object)
    return df


def store_instructors(df_json):
    os.makedirs(config.DATA_INSTRUCTORS_DIR, exist_ok=True)
    df_json.sort_values(by=['name'], inplace=True)
    df_json['departments'] = df_json['departments'] \
        .apply(to_sorted_list)
    df_json['classes'] = df_json['classes'] \
        .apply(to_sorted_list)

    df_csv = df_json.copy()
    df_csv['departments'] = df_csv['departments'] \
        .apply(lambda x: "\n".join(sorted(x)) if np.all(pd.notna(x)) else x)
    df_csv['classes'] = df_csv['classes'] \
        .apply(lambda x: ("\n".join([" ".join(sorted(cls)) for cls in x]) if np.all(pd.notna(x)) else x))
    remove_columns = ['culpa_reviews', 'gscholar']  # remove some columns from csv but leave in json
    for c in remove_columns:
        if c in df_csv.columns:
            df_csv = df_csv.drop([c], axis=1)

    store_df(config.DATA_INSTRUCTORS_DIR + '/instructors', df_json, df_csv)


def store_df(filename: str, df_json: pd.DataFrame, df_csv: pd.DataFrame):
    # store json
    file_json = open(filename + '.json', 'w')
    df_json.to_json(path_or_buf=file_json, orient="records", indent=2)
    file_json.close()

    # store csv
    file_csv = open(filename + '.csv', 'w')
    df_csv.to_csv(path_or_buf=file_csv, index=False)
    file_csv.close()


def to_sorted_list(x):
    return sorted(x) if np.all(pd.notna(x)) else x

