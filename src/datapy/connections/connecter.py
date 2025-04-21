#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File    :   connecter.py
@Time    :   2025/04/21 13:57:37
@Author  :   MuliMuri
@Version :   1.0
@Desc    :   To connect data server
'''


import pandas as pd

from itertools import chain
from typing import Any, Dict, Optional, Sequence, Tuple

from .mysql import MySQL, RetIndices
from ..query_builder import DataCondition, DataSelecter


def sql2df(sql_results: Tuple) -> Optional[pd.DataFrame]:
    if (not sql_results[RetIndices.STATUS]):
        return None

    return pd.DataFrame(
        data=chain(*sql_results[RetIndices.RESULT]),
        columns=sql_results[RetIndices.COLUMN_NAME],
        index=None
    )


def build_condition(conditions: Dict[str, DataCondition]) -> Dict[str, Tuple[str, Any]]:
    results = {}

    for dataset_item, dataset_condition in conditions.items():
        results[dataset_item] = dataset_condition.compile()

    return results


class DataConnecter():
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str
                 ):

        self.db = MySQL(host=host, port=port, user=user, password=password)

    def show(self, dataset_group: Optional[str]) -> Optional[pd.DataFrame]:
        if (dataset_group is None):
            return self.show_datasets()

        return self.show_items(dataset_group=dataset_group)

    def show_items(self, dataset_group: str) -> Optional[pd.DataFrame]:
        self.db.switch_database(dataset_group)

        results = self.db.execute(
            f"SELECT table_name AS Dataset_Item \
            FROM information_schema.tables \
            WHERE table_schema = '{dataset_group}'"
        )

        return sql2df(results)

    def show_datasets(self) -> Optional[pd.DataFrame]:
        results = self.db.execute(
            "SELECT schema_name AS Dataset_Group \
            FROM information_schema.schemata \
            WHERE schema_name NOT IN ('information_schema', 'performance_schema')"
        )

        return sql2df(results)

    def get(self,
            dataset_group: str,
            dataset_items: Sequence[str],
            dataset_conditions: Optional[Dict[str, DataCondition]] = None,
            refresh: bool = False
            ) -> Optional[Dict[str, pd.DataFrame]]:

        self.db.switch_database(dataset_group)

        if (dataset_conditions):
            keys = set(dataset_conditions.keys())

            # Global condition
            if ("GLOBAL" in keys):
                others = set(dataset_items).difference(keys)
                for other in others:
                    dataset_conditions[other] = dataset_conditions['GLOBAL']

                dataset_conditions.pop('GLOBAL')

        df_raws = {}

        for dataset_item in dataset_items:
            selecter = DataSelecter().select("*").from_table(dataset_item)

            if (dataset_conditions):
                selecter.where(dataset_conditions[dataset_item])

            results = self.db.execute(*selecter.build())

            if (not results[RetIndices.STATUS]):
                raise ValueError(f"CODE: {results[RetIndices.ERROR_CODE]} | MSG: {results[RetIndices.ERROR_MSG]}")

            df_raws[dataset_item] = pd.DataFrame(
                data=results[RetIndices.RESULT],
                columns=results[RetIndices.COLUMN_NAME],
                index=None
            )

        return df_raws
