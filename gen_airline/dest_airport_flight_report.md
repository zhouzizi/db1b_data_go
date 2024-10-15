## index索引名称

`dest_airport_flight_report`

## 字段说明

| 字段名                     | 描述                                                         |
|----------------------------|--------------------------------------------------------------|
| **airport**                 | 机场代码，表示机场的唯一标识代码，通常为IATA三字代码。        |
| **air_carrier**             | 航空公司代码，表示航空公司的唯一标识代码，通常为IATA两字代码。|
| **year**                    | 年份，表示航班数据所属的年份。                                |
| **month**                   | 月份，表示航班数据所属的月份（1-12）。                        |
| **flight_count**            | 总航班数，表示该机场或航空公司在特定时间段内的总航班数量。    |
| **early_departure_count**    | 提前出发航班数，表示在特定时间段内提前出发的航班数量。        |
| **delayed_departure_count**  | 延迟出发航班数，表示在特定时间段内延迟出发的航班数量。        |
| **delayed_15_departure_count** | 延迟15分钟出发航班数，表示延迟15分钟以上出发的航班数量。  |
| **early_arrival_count**      | 提前到达航班数，表示在特定时间段内提前到达的航班数量。        |
| **delayed_arrival_count**    | 延迟到达航班数，表示在特定时间段内延迟到达的航班数量。        |
| **delayed_15_arrival_count** | 延迟15分钟到达航班数，表示延迟15分钟以上到达的航班数量。      |
| **cancelled_count**          | 取消航班数，表示在特定时间段内取消的航班数量。                |

## Elasticsearch Mappings

```json
{
	"mappings": {
		"properties": {
			"airport": {
				"type": "keyword"
			},
			"air_carrier": {
				"type": "keyword"
			},
			"year": {
				"type": "short"
			},
			"month": {
				"type": "short"
			},
			"flight_count": {
				"type": "integer"
			},
			"early_departure_count": {
				"type": "integer"
			},
			"delayed_departure_count": {
				"type": "integer"
			},
			"delayed_15_departure_count": {
				"type": "integer"
			},
			"early_arrival_count": {
				"type": "integer"
			},
			"delayed_arrival_count": {
				"type": "integer"
			},
			"delayed_15_arrival_count": {
				"type": "integer"
			},
			"cancelled_count": {
				"type": "integer"
			}
		}
	}
}
