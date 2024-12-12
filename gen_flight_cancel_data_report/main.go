package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	ESUrl       = "http://127.0.0.1:9200/"
	bulkActions = 1000

	OnTimeDataIndexName             = "on_time_data"
	FlightCancelDataReportIndexName = "flight_cancel_data_report"
)

var (
	dates        = []Date{}
	actualNumCPU = runtime.GOMAXPROCS(0)
	esClient     *elastic.Client
)

func main() {
	dates = getDateConfig()
	if dates == nil {
		os.Exit(0)
	}
	fmt.Println("待处理数据时间为:", dates)
	connectES()
	initFlightCancelDataReportIndex()

	start := time.Now().Unix()
	for _, d := range dates {
		queryFlightCancelDataReport(d)
	}
	fmt.Println("总耗时", time.Now().Unix()-start, "s")
}

// 获取下载数据配置
func getDateConfig() []Date {
	var config = []Date{}
	f, err := os.Open("config.json")
	if err != nil {
		fmt.Println("读取配置文件失败:", err)
		return nil
	}
	defer f.Close()
	encoder := json.NewDecoder(f)
	err = encoder.Decode(&config)
	if err != nil {
		fmt.Println("解析配置文件失败:", err)
		return nil
	}
	return config
}

// 连接es数据库
func connectES() {
	var err error
	esClient, err = elastic.NewClient(
		// 设置ES服务地址，支持多个地址
		elastic.SetURL(ESUrl),
		elastic.SetSniff(false))
	if err != nil {
		// Handle error
		fmt.Println("ES连接失败: ", err)
		os.Exit(0)
	} else {
		fmt.Println("ES连接成功")
	}

}

// 创建索引
func initFlightCancelDataReportIndex() {
	ctx := context.Background()
	exists, err := esClient.IndexExists(FlightCancelDataReportIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断index是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		fmt.Println(FlightCancelDataReportIndexName, "索引已存在")
		return
	}
	mapping := `{
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
}`
	index, err := esClient.CreateIndex(FlightCancelDataReportIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建index失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建index.Acknowledged.no")
		os.Exit(0)
	}
	fmt.Println("initFlightCancelDataReportIndex成功")
}

func queryFlightCancelDataReport(d Date) {
	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", d.Year),
			elastic.NewTermQuery("month", d.Month),
		)
	// 定义复合聚合查询
	compositeAgg := elastic.NewCompositeAggregation().Size(2000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("year").Field("year"),
		elastic.NewCompositeAggregationTermsValuesSource("month").Field("month"),
		elastic.NewCompositeAggregationTermsValuesSource("reporting_airline").Field("reporting_airline"),
		elastic.NewCompositeAggregationTermsValuesSource("tail_number").Field("tail_number"))

	compositeAgg.
		SubAggregation("cancelled_carrier_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("cancelled", 1),
				elastic.NewTermQuery("cancellation_code", "A"),
			),
		)).
		SubAggregation("cancelled_weather_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("cancelled", 1),
				elastic.NewTermQuery("cancellation_code", "B"),
			),
		)).
		SubAggregation("cancelled_national_air_system_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("cancelled", 1),
				elastic.NewTermQuery("cancellation_code", "C"),
			),
		)).
		SubAggregation("cancelled_security_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("cancelled", 1),
				elastic.NewTermQuery("cancellation_code", "D"),
			),
		))

	ctx := context.Background()
	w, err := esClient.BulkProcessor().
		BulkActions(bulkActions).
		FlushInterval(time.Second).
		Workers(actualNumCPU).
		Stats(true).
		After(GetFailed).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	defer w.Close()
	if err = w.Start(ctx); err != nil {
		panic(err)
	}
	var cancelDataCount = 0
	// 执行查询
	var afterKey map[string]interface{}
	for {
		if afterKey != nil {
			compositeAgg = compositeAgg.AggregateAfter(afterKey)
		}

		searchResult, err := esClient.Search().
			Index(OnTimeDataIndexName). // 数据索引名
			Query(boolQuery).           // 查询条件
			Aggregation("composite_agg", compositeAgg.AggregateAfter(afterKey)).
			Size(0). // 不需要返回文档内容
			Do(ctx)
		if err != nil {
			panic(err)
		}

		// 处理并打印聚合结果
		compositeAggResult, _ := searchResult.Aggregations.Composite("composite_agg")
		for _, bucket := range compositeAggResult.Buckets {

			cancelled_carrier_count, _ := bucket.Aggregations.Filter("cancelled_carrier_count")
			cancelled_weather_count, _ := bucket.Aggregations.Filter("cancelled_weather_count")
			cancelled_national_air_system_count, _ := bucket.Aggregations.Filter("cancelled_national_air_system_count")
			cancelled_security_count, _ := bucket.Aggregations.Filter("cancelled_security_count")

			var r = &FlightCancelDataReport{}

			r.Year = cast.ToInt16(bucket.Key["year"])
			r.Month = cast.ToInt16(bucket.Key["month"])
			r.AirCarrier = cast.ToString(bucket.Key["reporting_airline"])
			r.TailNumber = cast.ToString(bucket.Key["tail_number"])
			r.FlightCount = cast.ToInt64(bucket.DocCount)
			r.CancelledCarrierCount = cast.ToInt64(cancelled_carrier_count.DocCount)
			r.CancelledWeatherCount = cast.ToInt64(cancelled_weather_count.DocCount)
			r.CancelledNationalAirSystemCount = cast.ToInt64(cancelled_national_air_system_count.DocCount)
			r.CancelledSecurityCount = cast.ToInt64(cancelled_security_count.DocCount)

			req := elastic.NewBulkIndexRequest().Index(FlightCancelDataReportIndexName).Id(strings.Join([]string{cast.ToString(r.Year), cast.ToString(r.Month), r.AirCarrier, r.TailNumber}, "_")).Doc(r)
			cancelDataCount++
			w.Add(req)
		}

		// 处理分页
		afterKey = compositeAggResult.AfterKey
		if compositeAggResult.AfterKey == nil {
			break
		}
	}
	for {
		st1 := w.Stats() //获取数据写入情况

		var finish = true
		for _, s := range st1.Workers {
			if s.Queued > 0 {
				finish = false
				break
			}
		}
		if finish {
			break
		}
	}

	fmt.Println("最后cancelDataCount:", cancelDataCount)

}

func GetFailed(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response == nil { //可能存在为空的情况 😳
		//log.Println("GetNil response return")
		return
	}
	fi := response.Failed()
	if len(fi) != 0 {
		for _, f := range fi {
			log.Printf("DebugFailedEs: index:%s type:%s id:%s version:%d  status:%d result:%s ForceRefresh:%v errorDetail:%v getResult:%v\n", f.Index, f.Type, f.Id, f.Version, f.Status, f.Result, f.ForcedRefresh, f.Error, f.GetResult)
			//panic(f.Error)
			return
		}
	}

}

type Date struct {
	Year  int
	Month int
}

type FlightCancelDataReport struct {
	Year                            int16  `json:"year"`                                // 年
	Month                           int16  `json:"month"`                               // 月
	AirCarrier                      string `json:"air_carrier"`                         // 航空公司代码
	TailNumber                      string `json:"tail_number"`                         // 航班号
	FlightCount                     int64  `json:"flight_count"`                        // 航班总数量
	CancelledCarrierCount           int64  `json:"cancelled_carrier_count"`             // 航空公司取消数量
	CancelledWeatherCount           int64  `json:"cancelled_weather_count"`             // 天气原因取消数量
	CancelledNationalAirSystemCount int64  `json:"cancelled_national_air_system_count"` // 国家航空系统取消数量
	CancelledSecurityCount          int64  `json:"cancelled_security_count"`            // 安全原因取消数量
}
