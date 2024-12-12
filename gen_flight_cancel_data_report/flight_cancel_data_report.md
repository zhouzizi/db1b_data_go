## 索引名称

`flight_cancel_data_report`

## 字段说明

| 字段名                                     | 描述                                                                 |
|-----------------------------------------|----------------------------------------------------------------------|
| **year**                                | 年份，表示航班记录的年份。                                            |
| **month**                               | 月份，表示航班记录的月份（1-12）。                                    |
| **air_carrier**                         | 航空公司代码，表示承运该航班的航空公司，通常为IATA两字代码。           |
| **tail_number**                         | 飞机尾号，表示航班所用飞机的唯一标识符。                               |
| **flight_count**                        | 航班总数，表示该记录对应的航班总数量。                                 |
| **cancelled_carrier_count**             | 航空公司原因取消的航班数量。                                           |
| **cancelled_weather_count**             | 天气原因取消的航班数量。                                               |
| **cancelled_national_air_system_count** | 国家空中系统问题导致取消的航班数量。                                   |
| **cancelled_security_count**            | 安全原因取消的航班数量。                                               |

## Elasticsearch Mappings

```json
{
  "mappings": {
    "properties": {
      "year": {
        "type": "short"
      },
      "month": {
        "type": "short"
      },
      "air_carrier": {
        "type": "keyword"
      },
      "tail_number": {
        "type": "keyword"
      },
      "flight_count": {
        "type": "integer"
      },
      "cancelled_carrier_count": {
        "type": "integer"
      },
      "cancelled_weather_count": {
        "type": "integer"
      },
      "cancelled_national_air_system_count": {
        "type": "integer"
      },
      "cancelled_security_count": {
        "type": "integer"
      }
    }
  }
}
