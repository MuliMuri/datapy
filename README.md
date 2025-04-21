# DataPy Documentation

DataPy is a lightweight data warehouse connector that enables effortless data retrieval from databases, automatically converting results to `DataFrame` for immediate use.

DataPy can simplifies constructing complex conditional queries and features local caching for rapid response to historical requests.

## Quick Start
```bash
git clone https://github.com/MuliMuri/datapy.git
cd datapy
python setup.py build
python setup.py install
```

```py
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
```
