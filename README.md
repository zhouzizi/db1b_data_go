# 流程概述

1. **`gen_flight_data`**：用于生成 `airport_flights` 数据，需先导入对应年份和季度的 `markets` 数据。

2. **`import_ontime`**：用于从BTS网站导入航班延误的源数据，数据导入基于`config.json`中的年月配置。
   
3. **`gen_airline`**：基于`config.json`配置，从`import_ontime`导入的数据中聚合出航班及延误数据。

4. **`gen_flight_cancel_data_report`**：用于生成 `flight_cancel_data_report` 数据，从`import_ontime`导入的数据中聚合出按年、月、航空公司、航班号 的航班取消情况。
 
5. **`gen_air_carrier_flight_report`**：用于生成 `air_carrier_flight_report` 数据，从`import_ontime`导入的数据中聚合出按年、月、航空公司 的航班延误情况。
# 执行流程

- 首先运行 `import_ontime`，完成数据导入后，执行 `gen_airline` 进行数据聚合。
- `gen_airline` 完成后，将生成三张数据表，分别为：

  - **`airlines`**：航线表，用于查询城市间存在的航线。
  - **`origin_airport_flight_report`**：出发延误报表。
  - **`dest_airport_flight_report`**：到达延误报表。

- 而后分别再执行`gen_flight_cancel_data_report` 和 `gen_air_carrier_flight_report`,分别生成：

  - **`flight_cancel_data_report`
  - **`air_carrier_flight_report`

- 若需生成 `airport_flights` 数据，请运行 `gen_flight_data`，确保先导入了指定年份和季度的 `markets` 数据。

# 索引管理

- 无需手动创建索引，代码中已经做了判断，索引不存在时会自动创建。
