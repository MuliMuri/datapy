Filter
======

Sometimes, you may have too many data entries and you want to filter some of them out to fetch filtered data items.
DataPy provides native SQL-based filter wrappers featuring operator overloading for simplified build of conditions.

Field
-----

DataPy utilizes field-centric filtering where all operations require Field class instantiation to specify target columns.

Filters are constructed using logical operators (<, <=, ==, >=, >) and subsequently passed to the get method.

Supported Operator Overload
---------------------------

.. list-table:: Operator Overload
   :widths: 15 25
   :header-rows: 1
   :class: comparison-table

   * - Python Operator
     - SQL Operator
   * - ``==``
     - ``=``
   * - ``!=``
     - ``!=``
   * - ``<``
     - ``<``
   * - ``<=``
     - ``<=``
   * - ``>``
     - ``>``
   * - ``>=``
     - ``>=``
   * - ``&``
     - ``AND``
   * - ``|``
     - ``OR``
   * - ``~``
     - ``NOT``

Example
-------

For example, this dataset is named ``weather_area1`` and is located in the ``weathers`` data group

+------------+-------------+--------------+------------+
| datetime   | temperature | humidity (%) | Wind (m/s) |
+============+=============+==============+============+
| 2024-03-01 | 18.5        | 62           | 3.2        |
+------------+-------------+--------------+------------+
| 2024-03-02 | 22.0        | 58           | 2.8        |
+------------+-------------+--------------+------------+
| 2024-03-03 | 15.3        | 85           | 5.6        |
+------------+-------------+--------------+------------+
| 2024-03-04 | 26.7        | 41           | 4.1        |
+------------+-------------+--------------+------------+
| 2024-03-05 | -5.4        | 93           | 9.3        |
+------------+-------------+--------------+------------+

Next, define filter fields by ``DataField``

.. code-block:: python

    import datapy as dp

    # Connect
    ...

    # Define fields
    datetime = dp.DataField("datetime")
    temperature = dp.DataField("temperature")

Filter data with a ``datetime`` between 2024-03-01 and 2024-03-03 and a ``temperature`` >= 18.0

.. code-block:: python

    # Build condition
    condition = datetime.between("2024-03-01", "2024-03-03") & (temperature >= 18.0)

    # Get the filtered data
    filtered_data = dp.get(
        dataset_group="weathers",
        dataset_items=['weather_area1'],
        dataset_conditions={
            'weather_area1': condition
        }
    )

    print(filtered_data)

If all success, you well see

+------------+-------------+--------------+------------+
| datetime   | temperature | humidity (%) | Wind (m/s) |
+============+=============+==============+============+
| 2024-03-01 | 18.5        | 62           | 3.2        |
+------------+-------------+--------------+------------+
| 2024-03-02 | 22.0        | 58           | 2.8        |
+------------+-------------+--------------+------------+
