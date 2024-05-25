import datetime
import os
import random
import re
import scrapy
from typing import List, Iterable
import pandas as pd

from cu_catalog import config


def split_term(term_str):
    search = re.search('(\w+)(\d{4})', term_str, re.IGNORECASE)
    assert search
    month = search.group(1)
    year = search.group(2)
    return month.capitalize(), year


def normalize_term(term_str):
    """
    Converts term used in URLs to unified form.

    >>> normalize_term("Summer2020")
    '2020-Summer'
    >>> normalize_term("SPRING2020")
    '2020-Spring'
    >>> normalize_term("Fall2019")
    '2019-Fall'
    """
    month, year = split_term(term_str)
    return year + "-" + month


def spider_run_loop(spider: scrapy.Spider, iterator: Iterable, _yield_func):
    """Runs a loop over iterator limiting it according to test_run spider option"""
    test_run = getattr(spider, 'test_run', False)
    num = 0
    for x in iterator:
        yield _yield_func(x)
        num += 1
        if test_run and num > 20:
            break


class InstructorsInternalDb:
    def __init__(self, datetime_fields: List[str]):
        self.df_internal = InstructorsInternalDb._load_internal_instructors_db(datetime_fields)
        self.datetime_fields = datetime_fields

    @staticmethod
    def _load_internal_instructors_db(datetime_fields: List[str]):
        assert 'name' not in datetime_fields
        if os.path.exists(config.DATA_INSTRUCTORS_INTERNAL_INFO_JSON):
            df = pd.read_json(config.DATA_INSTRUCTORS_INTERNAL_INFO_JSON, lines=True)
            # add if fields absent
            for field in datetime_fields:
                if field not in df.columns:
                    df[field] = pd.NaT
            # convert to datetime
            for field in datetime_fields:
                df[field] = pd.to_datetime(df[field], unit='ms')
            return df
        else:
            cols = ['name']
            cols.extend(datetime_fields)
            return pd.DataFrame(columns=cols)

    def update_instructor(self, name: str, field: str):
        assert field in self.datetime_fields
        if name not in self.df_internal['name'].values:
            new_row = pd.DataFrame([{'name': name}])
            self.df_internal = pd.concat([self.df_internal, new_row], ignore_index=True)
        self.df_internal.loc[self.df_internal['name'] == name, field] = datetime.datetime.now()

    def check_its_time(self, name: str, field: str, days_min: int, days_max: int):
        if name not in self.df_internal['name'].values:
            return True
        dt = self.df_internal.loc[self.df_internal['name'] == name, field].iloc[0]
        return InstructorsInternalDb.recent_threshold(dt, days_min, days_max)

    def store(self):
        os.makedirs(config.DATA_INTERNAL_DB_DIR, exist_ok=True)
        with open(config.DATA_INSTRUCTORS_INTERNAL_INFO_JSON, 'w') as file_json:
            self.df_internal.sort_values(by=['name'], inplace=True)
            self.df_internal.to_json(path_or_buf=file_json, orient="records", lines=True)

    @staticmethod
    def recent_threshold(x, days_min: int, days_max: int):
        if pd.isna(x):
            return True
        return x < datetime.datetime.now() - datetime.timedelta(days=random.randint(days_min, days_max))
