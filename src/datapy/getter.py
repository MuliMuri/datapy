'''
@File    :   getter.py
@Time    :   2025/04/21 13:44:22
@Author  :   MuliMuri
@Version :   1.0
@Desc    :   Data Getter Top Package
'''


import pandas as pd

from typing import Dict, Sequence, Optional

from .connections import DataConnecter
from .query_builder import DataCondition


class DataGetter:
    conn: Optional[DataConnecter] = None

    @classmethod
    def connect(cls,
                host: str,
                user: str,
                password: str,
                port: int = 3306
                ):

        cls.conn = DataConnecter(
            host=host,
            port=port,
            user=user,
            password=password
        )

    @classmethod
    def disconnect(cls):
        if (cls.conn is None):
            return

        del cls.conn
        cls.conn = None

    @classmethod
    def show(cls, dataset_group: Optional[str] = None) -> Optional[pd.DataFrame]:
        return cls.conn.show(dataset_group=dataset_group)

    @classmethod
    def get(cls,
            dataset_group: str,
            dataset_items: Sequence[str],
            dataset_conditions: Optional[Dict[str, DataCondition]] = None,
            refresh: bool = False
            ) -> Optional[pd.DataFrame]:

        return cls.conn.get(dataset_group=dataset_group,
                            dataset_items=dataset_items,
                            dataset_conditions=dataset_conditions,
                            refresh=refresh
                            )


def connect(host: str,
            user: str,
            password: str,
            port: int = 3306
            ) -> None:
    """To connect data warehouse

    Args:
        host (str): Hostname
        user (str): username
        password (str): password
        port (int, optional): Your data warehouse port. Defaults to 3306.
    """

    return DataGetter.connect(
        host=host,
        user=user,
        password=password,
        port=port
    )


def disconnect():
    """To disconnect data warehouse
    """

    return DataGetter.disconnect()


def show(dataset_group: Optional[str] = None) -> Optional[pd.DataFrame]:
    """To show available sets

    Args:
        dataset_group (Optional[str], optional):    If set None, it will return available dataset_groups, \
                                                    otherwise return available dataset_items. Defaults to None.

    Returns:
        Optional[pd.DataFrame]: Return DataFrame if it success.
    """
    return DataGetter.show(dataset_group=dataset_group)


def get(dataset_group: str,
        dataset_items: Sequence[str],
        dataset_conditions: Optional[Dict[str, DataCondition]] = None,
        refresh: bool = False
        ) -> Optional[pd.DataFrame]:
    """Fetch data from data warehouse

    Args:
        dataset_group (str): Appoint dataset group
        dataset_items (Sequence[str]): A sequence object with dataset_items
        dataset_conditions (Optional[Dict[str, DataCondition]], optional): Conditions. Defaults to None.
        refresh (bool, optional): Reserved. Defaults to False.

    Returns:
        Optional[pd.DataFrame]: Return DataFrame if it success.
    """

    return DataGetter.get(
        dataset_group=dataset_group,
        dataset_items=dataset_items,
        dataset_conditions=dataset_conditions,
        refresh=refresh
    )
