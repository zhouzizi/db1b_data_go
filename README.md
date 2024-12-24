# Elasticsearch数据抓取脚本使用

## 源数据导入

### 开发环境准备
在开始导入源数据前，需要确保开发环境已准备妥当。

1. **开发语言**：Go版本需为1.21及以上，安装教程参考：[Go安装教程](https://www.php.cn/faq/655417.html)
2. **数据库**：Elasticsearch版本为8.15，Win环境推荐使用Docker安装：[Elasticsearch Docker安装指南](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html)
3. **代码编辑器**：Goland安装最新版本即可。
4. **子豪的Admin后台**：确保已在本地部署并正常运行。

### 源数据结构和聚合数据结构说明在这个文件同一路径下，里面有相关字段及数据说明

### 注意事项
在开始运行脚本前，建议将Elasticsearch的账号和密码置为空，以避免额外的代码调整。

### 数据导入
1. **`markets`数据**
   - 通过子豪的管理后台导入。
   - 在管理后台的任务菜单中，选择指定季度的数据，点击下载即可开始自动导入。
   - 等待任务执行完成。建议同一时间只开启一个下载任务。
![c1ca2fe39a2e7aa4278ae12f1e745dc](https://github.com/user-attachments/assets/21576cad-1968-49ca-ac09-5e8b2a29b7c6)

2. **`on_time_data`数据**
   - 运行`import_ontime`项目进行导入。
   - 修改该项目下的`config.json`文件以配置需要导入的数据。
   - 如果Elasticsearch中已有对应年月的数据，系统会清空后重新导入。

## 聚合数据生成

### 配置要求
在运行聚合脚本之前，请确保所有`gen`脚本的`config.json`配置与对应的源数据时间一致。
例如：如果`markets`数据导入了2020年第一季度和第二季度，那么基于`markets`数据的脚本项目中的`config.json`也需要配置为2020年第一季度和第二季度。

### 前置条件
请先在子豪的Admin后台中，执行以下3条数据导入指令：
- `import_city` 导入城市数据
- `import_airport` 导入机场数据
- `import_air_carrier` 导入航司数据
![6f26a3f9f5d35c07d67538cfdc20af2](https://github.com/user-attachments/assets/bbe8e7fd-1ace-4433-8d93-8798e607d54e)

### 聚合脚本与生成的索引

1. **基于`markets`数据**
   - **生成索引**：`airport_flights`
   - **运行项目**：`gen_flight_data`

2. **基于`on_time_data`数据**
   - **生成索引**：
     - `airlines`（由`gen_airlines`项目生成）
     - `origin_airport_flight_report`（由`gen_airport_flight_report`项目生成）
     - `dest_airport_flight_report`（由`gen_airport_flight_report`项目生成）
     - `air_carrier_flight_report`（由`gen_air_carrier_flight_report`项目生成）
     - `flight_cancel_data_report`（由`gen_flight_cancel_data_report`项目生成）

### 执行顺序
基于`on_time_data`数据的聚合脚本可以不按特定顺序执行，但运行任一脚本前先检查下脚本项目下`config.json`中的配置，确保正确无误。

