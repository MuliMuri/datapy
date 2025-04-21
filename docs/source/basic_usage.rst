Basic Usage
===========

You can simply use DataPy by

.. code-block:: python

    import datapy as dp

    # Connect to data warehouse
    dp.connect(
        host="your_mysql_host",
        user="your_mysql_user",
        password="your_mysql_pass"
    )

    # To show available datagroups (database)
    datagroups_df = dp.show()

    # To show available datasets (table)
    datasets_df = dp.show(dataset_group="your_data_group")

    # Get the data
    data_df = dp.get(
        dataset_group="your_data_group",
        dataset_items=['dataset1', 'dataset2']  # [table1, table2]
    )

    # !BINGO! Enjoy your DataFrame time! :>
    print(data_df['dataset1'])
    print(data_df['dataset2'])
