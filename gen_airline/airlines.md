## 索引名称

`airlines`

## 字段说明

| 字段名                | 描述                               |
|--------------------|----------------------------------|
| **origin_airport** | 出发机场代码，表示航班的起始机场，通常为IATA三字代码。    |
| **origin_city**    | 出发城市名称，表示航班的起始城市。                |
| **dest_airport**   | 到达机场代码，表示航班的目的机场，通常为IATA三字代码。    |
| **dest_city**      | 到达城市名称，表示航班的目的城市。                |
| **air_carrier**    | 航空公司代码，表示承运该航班的航空公司，通常为IATA两字代码。 |
| **tail_number**    | 飞机尾号，表示航班所用飞机的唯一标识符。             |
| **domestic**            | 是否为国内航班，该航班的出发地目的地均为美国国内 。       |

## Elasticsearch Mappings

```json
{
    "mappings": {
        "properties": {
            "origin_airport": {
                "type": "keyword"
            },
            "origin_city": {
                "type": "keyword"
            },
            "dest_airport": {
                "type": "keyword"
            },
            "dest_city": {
                "type": "keyword"
            },
            "air_carrier": {
                "type": "keyword"
            },
            "tail_number": {
                "type": "keyword"
            },
            "domestic":{
                "type":"boolean"
            }
        }
    }
}
```
