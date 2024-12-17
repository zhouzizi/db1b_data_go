package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	//ESUrl       = "http://127.0.0.1:9200/"
	bulkActions = 1000

	CityInfoIndexName                  = "city_info"
	OnTimeDataIndexName                = "on_time_data"
	AirlinesIndexName                  = "airlines"
	OriginAirportFlightReportIndexName = "origin_airport_flight_report"

	DestAirportFlightReportIndexName = "dest_airport_flight_report"
)

var (
	config       = Config{}
	actualNumCPU = runtime.GOMAXPROCS(0)
	esClient     *elastic.Client
	airportMap   = map[string]string{} // key:机场代码 value:机场名称
	airlineMap   = map[string]string{} // key:航空公司代码 value:公司名称
	cityInfoMap  = map[string]bool{}   //key：city_market_id ,value:domestic
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

	readCityInfoIndexData()

	initAirCarrierIndex()
	initOriginReportsIndex()
	initDestReportsIndex()
	//读取机场代码信息
	readAirportCode()
	//读取机航空公司信息
	readAirlineCode()

	start := time.Now().Unix()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		start1 := time.Now().Unix()
		defer wg.Done()
		for _, d := range config.Dates {
			queryAirline(d)
		}
		fmt.Println("航班信息添加耗时", time.Now().Unix()-start1, "s")
	}()
	wg.Add(1)
	go func() {
		start2 := time.Now().Unix()
		defer wg.Done()
		for _, d := range config.Dates {
			queryOriginDelays(d)
			queryDestDelays(d)
		}
		fmt.Println("延误信息添加耗时", time.Now().Unix()-start2, "s")
	}()
	wg.Wait()
	fmt.Println("总耗时", time.Now().Unix()-start, "s")
}
func readCityInfoIndexData() {
	ctx := context.Background()
	searchResult, err := esClient.Search().Index(CityInfoIndexName).Size(10000).Do(ctx)
	if err != nil {
		fmt.Println("读取", CityInfoIndexName, "失败:", err)
		os.Exit(0)
	}

	var count = 0
	for _, hit := range searchResult.Hits.Hits {
		var info CityInfo
		if e := json.Unmarshal(hit.Source, &info); e != nil {
			fmt.Println("readCityInfoIndexData.Unmarshal.err", err)
			panic(e)
		}
		cityInfoMap[info.Code] = info.Domestic
		count++
	}

	if count != len(cityInfoMap) {
		fmt.Println("cityInfoCount", count, "map len:", len(cityInfoMap))
		os.Exit(0)
	}
	if count == 0{
		fmt.Println("cityInfoCount nil")
		os.Exit(0)
	}
	fmt.Println("load cityInfo ok.")
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

// 创建索引
func initAirCarrierIndex() {
	ctx := context.Background()
	exists, err := esClient.IndexExists(AirlinesIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断index是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		fmt.Println(AirlinesIndexName, "索引已存在")
		return
	}
	mapping := `{
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
            }
        }
    }
}`
	index, err := esClient.CreateIndex(AirlinesIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建index失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建index.Acknowledged.no")
		os.Exit(0)
	}
	fmt.Println("initAirlinesIndex成功")
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

// 读取机场到内存
func readAirportCode() {
	f, e := os.Open("L_AIRPORT.csv")
	if e != nil {
		panic(e)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	var n = 0
	var all = 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		all++
		if err != nil {
			panic(err)
		}
		if record[0] != "Code" {
			n++
			arr := strings.Split(record[1], ": ")
			if len(arr) < 2 {
				airportMap[record[0]] = record[1]
			} else {
				airportMap[record[0]] = arr[1]
			}

		}
	}
	fmt.Println("读取机场完成:", n)
}
func readAirlineCode() {
	f, e := os.Open("L_UNIQUE_CARRIERS.csv")
	if e != nil {
		panic(e)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	var n = 0
	var all = 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		all++
		if err != nil {
			panic(err)
		}
		if record[0] != "Code" {
			n++
			airlineMap[record[0]] = record[1]

		}
	}
	fmt.Println("读取航空公司完成:", n)
}

func queryAirline(d Date) {
	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", d.Year),
			elastic.NewTermQuery("month", d.Month),
		)
	// 定义复合聚合查询
	compositeAgg := elastic.NewCompositeAggregation().Size(2000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("origin").Field("origin"),
		elastic.NewCompositeAggregationTermsValuesSource("dest").Field("dest"),
		elastic.NewCompositeAggregationTermsValuesSource("tail_number").Field("tail_number"))
	// 定义子聚合（top_hits用于获取origin_country和origin_state）
	topHitsAgg := elastic.NewTopHitsAggregation().
		Size(1). // 只需要返回1条记录
		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("origin_city_name", "dest_city_name", "origin_city_market_id", "dest_city_market_id", "reporting_airline"))

	var afterKey map[string]interface{}

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
	var count = 0
	for {
		// 每次查询时将 afterKey 传递给聚合
		if afterKey != nil {
			compositeAgg = compositeAgg.AggregateAfter(afterKey)
		}
		// 组合查询
		searchResult, err := esClient.Search().
			Index(OnTimeDataIndexName). // 索引名称
			Query(boolQuery).           // 添加查询条件
			Size(0).                    // 我们不需要返回文档，设置为0
			Aggregation("unique_routes", compositeAgg.SubAggregation("route_info", topHitsAgg)).
			Do(ctx)
		if err != nil {
			panic(err)
		}

		// 处理结果
		agg, _ := searchResult.Aggregations.Composite("unique_routes")
		for _, bucket := range agg.Buckets {
			//count++
			al := &Airline{}
			al.OriginAirport = airportMap[cast.ToString(bucket.Key["origin"])]
			al.DestAirport = airportMap[cast.ToString(bucket.Key["dest"])]
			al.TailNumber = cast.ToString(bucket.Key["tail_number"])

			topHits, _ := bucket.Aggregations.TopHits("route_info")
			hit := topHits.Hits.Hits[0]

			var source map[string]interface{}
			_ = json.Unmarshal(hit.Source, &source)
			al.OriginCity = cast.ToString(source["origin_city_name"]) //TODO:城市名和state缩写分开
			al.DestCity = cast.ToString(source["dest_city_name"])
			al.AirCarrier = airlineMap[cast.ToString(source["reporting_airline"])]

			origin_city_market_id := cast.ToString(source["origin_city_market_id"])
			dest_city_market_id := cast.ToString(source["dest_city_market_id"])
			originDomestic, _ := cityInfoMap[origin_city_market_id]
			destDomestic, _ := cityInfoMap[dest_city_market_id]
			if originDomestic && destDomestic {
				al.Domestic = true
			} else {
				al.Domestic = false
			}

			req := elastic.NewBulkIndexRequest().Index(AirlinesIndexName).Id(strings.Join([]string{cast.ToString(bucket.Key["origin"]), cast.ToString(bucket.Key["dest"]), cast.ToString(bucket.Key["tail_number"])}, "_")).Doc(al)
			count++
			w.Add(req)
		}
		afterKey = agg.AfterKey
		if agg.AfterKey == nil {
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
	fmt.Println("最后数量", count)
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
type Airline struct {
	OriginAirport string `json:"origin_airport"`
	OriginCity    string `json:"origin_city"`
	DestAirport   string `json:"dest_airport"`
	DestCity      string `json:"dest_city"`
	AirCarrier    string `json:"air_carrier"`
	Domestic      bool   `json:"domestic"`
	TailNumber    string `json:"tail_number"`
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
type CityInfo struct {
	Name     string `json:"name"`
	State    string `json:"state"`
	Code     string `json:"code"`
	Domestic bool   `json:"domestic"`
}
