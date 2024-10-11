# db1b_data_go
# 流程概述

1. **`import_ontime`**：用于从BTS网站导入航班延误的源数据，数据导入基于`config.json`中的年月配置。
   
2. **`gen_airline`**：基于`config.json`配置，从`import_ontime`导入的数据中聚合出航班及延误数据。

# 执行流程

- 首先运行 `import_ontime`，完成数据导入后，执行 `gen_airline` 进行数据聚合。
- `gen_airline` 完成后，将生成三张数据表，分别为：

  - **`airlines`**：航线表，用于查询城市间存在的航线。
  - **`origin_ontime_report`**：出发延误报表。
  - **`dest_ontime_report`**：到达延误报表。

# 索引管理

- 三张表的索引结构分别由以下方法自动创建：
  - `initAirlinesIndex()`
  - `initOriginReportsIndex()`
  - `initDestReportsIndex()`
- 无需手动创建索引，代码中已经做了判断，索引不存在时会自动创建。
