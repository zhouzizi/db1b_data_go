package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	bulkActions                        = 1000
	OnTimeDataIndexName                = "on_time_data"
	OriginAirportFlightReportIndexName = "origin_airport_flight_report"
	DestAirportFlightReportIndexName   = "dest_airport_flight_report"
)

var (
	config       = Config{}
	actualNumCPU = runtime.GOMAXPROCS(0)
	esClient     *elastic.Client
)

type Config struct {
	Dates []Date `json:"dates"`
	Es    `json:"elasticsearch"`
}
type Es struct {
	Url      string
	Username string
	Password string
}

func main() {
	config = getDateConfig()
	if len(config.Dates) == 0 || config.Es.Url == "" {
		fmt.Println("配置文件错误")
		os.Exit(0)
	}
	fmt.Println("待处理数据时间为:", config.Dates)
	connectES()

	initOriginReportsIndex()
	initDestReportsIndex()

	start := time.Now().Unix()
	for _, d := range config.Dates {
		queryOriginDelays(d)
		queryDestDelays(d)
	}
	fmt.Println("延误信息总耗时", time.Now().Unix()-start, "s")
}

// 获取下载数据配置
func getDateConfig() Config {
	execDir, err := os.Getwd() // 获取当前工作目录
	if err != nil {
		fmt.Println("Error getting current working directory:", err)
		panic(err)
	}
	// 拼接同级目录下的  文件路径
	filePath := filepath.Join(execDir, "config.json")
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println("读取配置文件失败:", err)
		panic(err)
	}
	defer f.Close()
	encoder := json.NewDecoder(f)
	err = encoder.Decode(&config)
	if err != nil {
		fmt.Println("解析配置文件失败:", err)
		panic(err)
	}
	return config
}

// 连接es数据库
func connectES() {
	var err error
	esClient, err = elastic.NewClient(
		// 设置ES服务地址，支持多个地址
		elastic.SetURL(config.Url),
		elastic.SetBasicAuth(config.Username, config.Password),
		elastic.SetSniff(false))
	if err != nil {
		// Handle error
		fmt.Println("ES连接失败: ", err)
		os.Exit(0)
	} else {
		fmt.Println("ES连接成功")
	}

}

func initOriginReportsIndex() {
	ctx := context.Background()
	exists, err := esClient.IndexExists(OriginAirportFlightReportIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断", OriginAirportFlightReportIndexName, "是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		// Index does not exist yet.
		fmt.Println(OriginAirportFlightReportIndexName, "索引已存在")
		return
	}
	mapping := `{
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
}`
	index, err := esClient.CreateIndex(OriginAirportFlightReportIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建", OriginAirportFlightReportIndexName, "失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建", OriginAirportFlightReportIndexName, ".no")
		os.Exit(0)
	}
	fmt.Println("init", OriginAirportFlightReportIndexName, "成功")
}
func initDestReportsIndex() {
	ctx := context.Background()
	exists, err := esClient.IndexExists(DestAirportFlightReportIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断", DestAirportFlightReportIndexName, "是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		// Index does not exist yet.
		fmt.Println(DestAirportFlightReportIndexName, "索引已存在")
		return
	}
	mapping := `{
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
}`
	index, err := esClient.CreateIndex(DestAirportFlightReportIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建", DestAirportFlightReportIndexName, "失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建", DestAirportFlightReportIndexName, ".no")
		os.Exit(0)
	}
	fmt.Println("init", DestAirportFlightReportIndexName, "成功")
}

func queryOriginDelays(d Date) {

	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", d.Year),
			elastic.NewTermQuery("month", d.Month),
		)

	// 使用 Composite Aggregation 按多个字段进行分组
	compositeAgg := elastic.NewCompositeAggregation().Size(2000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("year").Field("year"),
		elastic.NewCompositeAggregationTermsValuesSource("month").Field("month"),
		elastic.NewCompositeAggregationTermsValuesSource("reporting_airline").Field("reporting_airline"),
		elastic.NewCompositeAggregationTermsValuesSource("origin").Field("origin"),
	)

	// 添加子聚合统计准点航班、延迟航班、取消航班
	compositeAgg.SubAggregation("early_departure_count", elastic.NewFilterAggregation().Filter(
		elastic.NewBoolQuery().Must(
			elastic.NewRangeQuery("dep_delay").Lt(0),
			elastic.NewTermQuery("cancelled", 0),
		),
	)).
		SubAggregation("delayed_departure_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewRangeQuery("dep_delay").Gt(0),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("delayed_15_departure_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("dep_del15", 1),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("early_arrival_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewRangeQuery("arr_delay").Lt(0),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("delayed_arrival_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewRangeQuery("arr_delay").Gt(0),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("delayed_15_arrival_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("arr_del15", 1),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("cancelled_count", elastic.NewFilterAggregation().Filter(
			elastic.NewTermQuery("cancelled", 1),
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
	w.Start(ctx)
	defer w.Close()
	var originDelayCount = 0
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

			earlyDepartureCount, _ := bucket.Aggregations.Filter("early_departure_count")
			delayedDepartureCount, _ := bucket.Aggregations.Filter("delayed_departure_count")
			delayed15DepartureCount, _ := bucket.Aggregations.Filter("delayed_15_departure_count")
			earlyArrivalCount, _ := bucket.Aggregations.Filter("early_arrival_count")
			delayedArrivalCount, _ := bucket.Aggregations.Filter("delayed_arrival_count")
			delayed15ArrivalCount, _ := bucket.Aggregations.Filter("delayed_15_arrival_count")
			cancelledCount, _ := bucket.Aggregations.Filter("cancelled_count")

			var r = &OntimeAirportFlightReport{}
			r.Year = cast.ToInt64(bucket.Key["year"])
			r.Month = cast.ToInt64(bucket.Key["month"])
			r.Airport = cast.ToString(bucket.Key["origin"])
			r.AirCarrier = cast.ToString(bucket.Key["reporting_airline"])
			r.FlightCount = cast.ToInt64(bucket.DocCount)
			r.EarlyDepartureCount = cast.ToInt64(earlyDepartureCount.DocCount)
			r.DelayedDepartureCount = cast.ToInt64(delayedDepartureCount.DocCount)
			r.Delayed15DepartureCount = cast.ToInt64(delayed15DepartureCount.DocCount)
			r.EarlyArrivalCount = cast.ToInt64(earlyArrivalCount.DocCount)
			r.DelayedArrivalCount = cast.ToInt64(delayedArrivalCount.DocCount)
			r.Delayed15ArrivalCount = cast.ToInt64(delayed15ArrivalCount.DocCount)
			r.CancelledCount = cast.ToInt64(cancelledCount.DocCount)

			req := elastic.NewBulkIndexRequest().Index(OriginAirportFlightReportIndexName).Id(strings.Join([]string{cast.ToString(r.Year), cast.ToString(r.Month), r.AirCarrier, r.Airport}, "_")).Doc(r)
			originDelayCount++
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

	fmt.Println("最后originDelayCount:", originDelayCount)
}
func queryDestDelays(d Date) {

	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", d.Year),
			elastic.NewTermQuery("month", d.Month),
		)

	// 使用 Composite Aggregation 按多个字段进行分组
	compositeAgg := elastic.NewCompositeAggregation().Size(2000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("year").Field("year"),
		elastic.NewCompositeAggregationTermsValuesSource("month").Field("month"),
		elastic.NewCompositeAggregationTermsValuesSource("reporting_airline").Field("reporting_airline"),
		elastic.NewCompositeAggregationTermsValuesSource("dest").Field("dest"),
	)

	// 添加子聚合统计准点航班、延迟航班、取消航班
	compositeAgg.SubAggregation("early_departure_count", elastic.NewFilterAggregation().Filter(
		elastic.NewBoolQuery().Must(
			elastic.NewRangeQuery("dep_delay").Lt(0),
			elastic.NewTermQuery("cancelled", 0),
		),
	)).
		SubAggregation("delayed_departure_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewRangeQuery("dep_delay").Gt(0),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("delayed_15_departure_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("dep_del15", 1),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("early_arrival_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewRangeQuery("arr_delay").Lt(0),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("delayed_arrival_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewRangeQuery("arr_delay").Gt(0),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("delayed_15_arrival_count", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("arr_del15", 1),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("cancelled_count", elastic.NewFilterAggregation().Filter(
			elastic.NewTermQuery("cancelled", 1),
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
	w.Start(ctx)
	defer w.Close()
	var destDelayCount = 0
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

			earlyDepartureCount, _ := bucket.Aggregations.Filter("early_departure_count")
			delayedDepartureCount, _ := bucket.Aggregations.Filter("delayed_departure_count")
			delayed15DepartureCount, _ := bucket.Aggregations.Filter("delayed_15_departure_count")
			earlyArrivalCount, _ := bucket.Aggregations.Filter("early_arrival_count")
			delayedArrivalCount, _ := bucket.Aggregations.Filter("delayed_arrival_count")
			delayed15ArrivalCount, _ := bucket.Aggregations.Filter("delayed_15_arrival_count")
			cancelledCount, _ := bucket.Aggregations.Filter("cancelled_count")

			var r = &OntimeAirportFlightReport{}
			r.Year = cast.ToInt64(bucket.Key["year"])
			r.Month = cast.ToInt64(bucket.Key["month"])
			r.Airport = cast.ToString(bucket.Key["dest"])
			r.AirCarrier = cast.ToString(bucket.Key["reporting_airline"])
			r.FlightCount = cast.ToInt64(bucket.DocCount)
			r.EarlyDepartureCount = cast.ToInt64(earlyDepartureCount.DocCount)
			r.DelayedDepartureCount = cast.ToInt64(delayedDepartureCount.DocCount)
			r.Delayed15DepartureCount = cast.ToInt64(delayed15DepartureCount.DocCount)
			r.EarlyArrivalCount = cast.ToInt64(earlyArrivalCount.DocCount)
			r.DelayedArrivalCount = cast.ToInt64(delayedArrivalCount.DocCount)
			r.Delayed15ArrivalCount = cast.ToInt64(delayed15ArrivalCount.DocCount)
			r.CancelledCount = cast.ToInt64(cancelledCount.DocCount)

			req := elastic.NewBulkIndexRequest().Index(DestAirportFlightReportIndexName).Id(strings.Join([]string{cast.ToString(r.Year), cast.ToString(r.Month), r.AirCarrier, r.Airport}, "_")).Doc(r)
			destDelayCount++
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

	fmt.Println("最后DestDelayCount:", destDelayCount)
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

type OntimeAirportFlightReport struct {
	Airport                 string `json:"airport"`
	AirCarrier              string `json:"air_carrier"`
	Year                    int64  `json:"year"`
	Month                   int64  `json:"month"`
	FlightCount             int64  `json:"flight_count"`
	EarlyDepartureCount     int64  `json:"early_departure_count"`
	DelayedDepartureCount   int64  `json:"delayed_departure_count"`
	Delayed15DepartureCount int64  `json:"delayed_15_departure_count"`
	EarlyArrivalCount       int64  `json:"early_arrival_count"`
	DelayedArrivalCount     int64  `json:"delayed_arrival_count"`
	Delayed15ArrivalCount   int64  `json:"delayed_15_arrival_count"`
	CancelledCount          int64  `json:"cancelled_count"`
}
