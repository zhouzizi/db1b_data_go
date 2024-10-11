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
	"runtime"
	"strings"
	"time"
)

const (
	ESUrl       = "http://127.0.0.1:9200/"
	bulkActions = 1000

	OnTimeDataIndexName         = "on_time_data"
	AirlinesIndexName           = "airlines"
	OriginOntimeReportIndexName = "origin_ontime_report"
	DestOntimeReportIndexName   = "dest_ontime_report"
)

var (
	dates        = []Date{}
	actualNumCPU = runtime.GOMAXPROCS(0)
	esClient     *elastic.Client
	airportMap   = map[string]string{} // key:机场代码 value:机场名称
	airlineMap   = map[string]string{} // key:航空公司代码 value:公司名称
)

func main() {
	dates = getDateConfig()
	if dates == nil {
		os.Exit(0)
	}
	fmt.Println("待下载数据时间为:", dates)
	connectES()
	initAirlinesIndex()
	initOriginReportsIndex()
	initDestReportsIndex()
	//读取机场代码信息
	readAirportCode()
	//读取机航空公司信息
	readAirlineCode()
	//读取airlines
	start := time.Now().Unix()
	for _, d := range dates {
		queryAirline(d)
	}
	fmt.Println("航班信息添加耗时", time.Now().Unix()-start, "s")

	start = time.Now().Unix()
	for _, d := range dates {
		queryOriginDelays(d)
		queryDestDelays(d)
	}
	fmt.Println("延误信息添加耗时", time.Now().Unix()-start, "s")

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
func initAirlinesIndex() {
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
            "airline": {
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
	exists, err := esClient.IndexExists(OriginOntimeReportIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断OriginOntimeReportIndexName是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		// Index does not exist yet.
		fmt.Println(OriginOntimeReportIndexName, "索引已存在")
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
			"airport": {
				"type": "keyword"
			},
			"airport_name": {
				"type": "keyword"
			},
			"airline": {
				"type": "keyword"
			},
			"airline_name": {
				"type": "keyword"
			},
			"ontime_count": {
				"type": "integer"
			},
			"delayed_count": {
				"type": "integer"
			},
			"cancelled_count": {
				"type": "integer"
			},
			"flight_count": {
				"type": "integer"
			}
		}
	}
}`
	index, err := esClient.CreateIndex(OriginOntimeReportIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建OriginOntimeReportIndexName失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建OriginOntimeReportIndexName.no")
		os.Exit(0)
	}
	fmt.Println("OriginOntimeReportIndexName成功")
}
func initDestReportsIndex() {
	ctx := context.Background()
	exists, err := esClient.IndexExists(DestOntimeReportIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断DestOntimeReportIndexName是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		// Index does not exist yet.
		fmt.Println(DestOntimeReportIndexName, "索引已存在")
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
			"airport": {
				"type": "keyword"
			},
			"airport_name": {
				"type": "keyword"
			},
			"airline": {
				"type": "keyword"
			},
			"airline_name": {
				"type": "keyword"
			},
			"ontime_count": {
				"type": "integer"
			},
			"delayed_count": {
				"type": "integer"
			},
			"cancelled_count": {
				"type": "integer"
			},
			"flight_count": {
				"type": "integer"
			}
		}
	}
}`
	index, err := esClient.CreateIndex(DestOntimeReportIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建DestOntimeReportIndexName失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建DestOntimeReportIndexName.no")
		os.Exit(0)
	}
	fmt.Println("DestOntimeReportIndexName成功")
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
		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("origin_city_name", "dest_city_name", "reporting_airline"))

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
			al.OriginCity = cast.ToString(source["origin_city_name"])
			al.DestCity = cast.ToString(source["dest_city_name"])
			al.Airline = airlineMap[cast.ToString(source["reporting_airline"])]

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
	compositeAgg.SubAggregation("ontime_flights", elastic.NewFilterAggregation().Filter(
		elastic.NewBoolQuery().Must(
			elastic.NewTermQuery("dep_del15", 0),
			elastic.NewTermQuery("cancelled", 0),
		),
	)).
		SubAggregation("delayed_flights", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("dep_del15", 1),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("cancelled_flights", elastic.NewFilterAggregation().Filter(
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

			onTimeFlights, _ := bucket.Aggregations.Filter("ontime_flights")
			delayedFlights, _ := bucket.Aggregations.Filter("delayed_flights")
			cancelledFlights, _ := bucket.Aggregations.Filter("cancelled_flights")

			var r = &OntimeReport{}
			r.Year = cast.ToInt64(bucket.Key["year"])
			r.Month = cast.ToInt64(bucket.Key["month"])
			r.Airport = cast.ToString(bucket.Key["origin"])
			r.AirportName = airportMap[r.Airport]
			r.Airline = cast.ToString(bucket.Key["reporting_airline"])
			r.AirlineName = airlineMap[r.Airline]
			r.OntimeCount = cast.ToInt64(onTimeFlights.DocCount)
			r.DelayedCount = cast.ToInt64(delayedFlights.DocCount)
			r.CancelledCount = cast.ToInt64(cancelledFlights.DocCount)
			r.FlightCount = cast.ToInt64(bucket.DocCount)

			req := elastic.NewBulkIndexRequest().Index(OriginOntimeReportIndexName).Id(strings.Join([]string{cast.ToString(r.Year), cast.ToString(r.Month), r.Airline, r.Airport}, "_")).Doc(r)

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
}
func queryDestDelays(d Date) {

	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", d.Year),
			elastic.NewTermQuery("month", d.Month),
		)

	// 使用 Composite Aggregation 按多个字段进行分组
	compositeAgg := elastic.NewCompositeAggregation().Size(2000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("reporting_airline").Field("reporting_airline"),
		elastic.NewCompositeAggregationTermsValuesSource("dest").Field("dest"),
		elastic.NewCompositeAggregationTermsValuesSource("year").Field("year"),
		elastic.NewCompositeAggregationTermsValuesSource("month").Field("month"),
	)

	// 添加子聚合统计准点航班、延迟航班、取消航班
	compositeAgg.SubAggregation("ontime_flights", elastic.NewFilterAggregation().Filter(
		elastic.NewBoolQuery().Must(
			elastic.NewTermQuery("arr_del15", 0),
			elastic.NewTermQuery("cancelled", 0),
		),
	)).
		SubAggregation("delayed_flights", elastic.NewFilterAggregation().Filter(
			elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("arr_del15", 1),
				elastic.NewTermQuery("cancelled", 0),
			),
		)).
		SubAggregation("cancelled_flights", elastic.NewFilterAggregation().Filter(
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
		panic(err)
	}
	w.Start(ctx)
	defer w.Close()

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

			onTimeFlights, _ := bucket.Aggregations.Filter("ontime_flights")
			delayedFlights, _ := bucket.Aggregations.Filter("delayed_flights")
			cancelledFlights, _ := bucket.Aggregations.Filter("cancelled_flights")

			var r = &OntimeReport{}
			r.Year = cast.ToInt64(bucket.Key["year"])
			r.Month = cast.ToInt64(bucket.Key["month"])
			r.Airport = cast.ToString(bucket.Key["origin"])
			r.AirportName = airportMap[r.Airport]
			r.Airline = cast.ToString(bucket.Key["reporting_airline"])
			r.AirlineName = airlineMap[r.Airline]
			r.OntimeCount = cast.ToInt64(onTimeFlights.DocCount)
			r.DelayedCount = cast.ToInt64(delayedFlights.DocCount)
			r.CancelledCount = cast.ToInt64(cancelledFlights.DocCount)
			r.FlightCount = cast.ToInt64(bucket.DocCount)

			req := elastic.NewBulkIndexRequest().Index(DestOntimeReportIndexName).Id(strings.Join([]string{cast.ToString(r.Year), cast.ToString(r.Month), r.Airline, r.Airport}, "_")).Doc(r)

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
	Airline       string `json:"airline"`
	TailNumber    string `json:"tail_number"`
}
type OntimeReport struct {
	Year           int64  `json:"year"`
	Month          int64  `json:"month"`
	Airport        string `json:"airport"`
	AirportName    string `json:"airport_name"`
	Airline        string `json:"airline"`
	AirlineName    string `json:"airline_name"`
	OntimeCount    int64  `json:"ontime_count"`
	DelayedCount   int64  `json:"delayed_count"`
	CancelledCount int64  `json:"cancelled_count"`
	FlightCount    int64  `json:"flight_count"`
}
