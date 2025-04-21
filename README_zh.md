# DataPy

DataPy 是一个轻量级数据仓库连接器，支持从数据库中轻松检索数据，自动将结果转换为“数据帧”以供立即使用。

DataPy 可以简化复杂条件查询的构建，并具有本地缓存功能，可快速响应历史请求。

## 快速使用
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
