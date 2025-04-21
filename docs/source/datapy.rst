DataPy
==========

.. py:method:: connect (host: str, user: str, password: str, port: int = 3306) -> None

    :param str host: Database host address
    :param str user: Authentication username
    :param str password: Authentication password
    :param int port: Connection port number (default: 3306)

    Establish connection to the data warehouse

    .. code-block:: python

        DataGetter.connect(
            host="localhost",
            user="admin",
            password="secret",
            port=3306
        )

.. py:method:: disconnect () -> None

    Terminate current connection and cleanup resources

.. py:method:: show (dataset_group: Optional[str] = None) -> Optional[pd.DataFrame]

    :param Optional[str] dataset_group:
        - If ``None``: returns available dataset groups
        - If specified: returns items in the specified group

    :return: DataFrame with available datasets or None if connection inactive
    :rtype: Optional[pd.DataFrame]

    Display available datasets

.. py:method:: get (dataset_group: str, dataset_items: Sequence[str], dataset_conditions: Optional[Dict[str, DataCondition]] = None, refresh: bool = False) -> Optional[pd.DataFrame]

    :param str dataset_group: Target dataset group identifier
    :param Sequence[str] dataset_items: List of dataset items to retrieve
    :param Optional[Dict[str, DataCondition]] dataset_conditions:
        Dictionary of filtering conditions (default: None)
    :param bool refresh:
        Bypass cache and force fresh retrieval (default: False)

    :return: Requested data as DataFrame or None if retrieval fails
    :rtype: Optional[pd.DataFrame]

    Retrieve data from warehouse
