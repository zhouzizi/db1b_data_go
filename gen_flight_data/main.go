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

var (
	client *elastic.Client
)

// key:城市id value:城市名称
var cityMap = map[string]string{}

// key:机场代码 value:机场名称
var airportMap = map[string]string{}

var market_index_name = "markets"
var airport_flights_index_name = "airport_flights"

var actualNumCPU = runtime.GOMAXPROCS(0)
var bulkActions = 1000

type DateArg struct {
	Year    int
	Quarter int
}

func main() {
	//连接数据库
	connectEs()
	initFlightsIndex()
	//// 设置要使用的最大CPU核心数
	runtime.GOMAXPROCS(actualNumCPU)
	fmt.Println("CPU核心数:", actualNumCPU)
	//读取机场和地区信息
	readCityMarketID()
	readAirportCode()

	arr := getDataConfig()
	if arr == nil || len(arr) == 0 {
		fmt.Println("配置文件解析失败")
		os.Exit(0)
	}
	start := time.Now().Unix()
	for _, tt := range arr {
		processFlightsData(tt.Year, tt.Quarter)
	}
	fmt.Println("总耗时", time.Now().Unix()-start, "s")
}
func processFlightsData(year, quarter int) {
	ctx := context.Background()
	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", year),
			elastic.NewTermQuery("quarter", quarter),
		)
	// 定义复合聚合查询
	compositeAgg := elastic.NewCompositeAggregation().Size(10000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("origin").Field("origin"),
		elastic.NewCompositeAggregationTermsValuesSource("dest").Field("dest"))
	// 子聚合，用于计算平均票价和乘客总数
	avgFareAgg := elastic.NewAvgAggregation().Field("mkt_fare").Missing(0)      // 当 mkt_fare 缺失时，使用 0 计算平均值
	passengersAgg := elastic.NewSumAggregation().Field("passengers").Missing(0) // 当 passengers 缺失时，使用 0 计算总和
	compositeAgg = compositeAgg.
		SubAggregation("average_fare", avgFareAgg).
		SubAggregation("total_passengers", passengersAgg)
	// 定义子聚合（top_hits用于获取origin_country和origin_state）
	topHitsAgg := elastic.NewTopHitsAggregation().
		Size(1). // 只需要返回1条记录
		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("origin_city_market_id", "origin_state", "origin_state_name", "origin_country", "dest_city_market_id", "dest_state", "dest_state_name", "dest_country"))

	w, err := client.BulkProcessor().
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

	var afterKey map[string]interface{}
	var count = 0
	for {
		// 每次查询时将 afterKey 传递给聚合
		if afterKey != nil {
			compositeAgg = compositeAgg.AggregateAfter(afterKey)
		}
		// 组合查询
		searchResult, err := client.Search().
			Index(market_index_name). // 索引名称
			Query(boolQuery).         // 添加查询条件
			Size(0).                  // 我们不需要返回文档，设置为0
			Aggregation("unique_routes", compositeAgg.SubAggregation("route_info", topHitsAgg)).
			Do(ctx)
		if err != nil {
			panic(err)
		}

		// 处理结果
		agg, _ := searchResult.Aggregations.Composite("unique_routes")
		for _, bucket := range agg.Buckets {
			count++
			af := &AirportFlight{}
			af.Year = year
			af.Quarter = quarter
			af.OriginAirport = cast.ToString(bucket.Key["origin"])
			af.DestAirport = cast.ToString(bucket.Key["dest"])
			//af.FlightNum = cast.ToInt(bucket.DocCount)

			avgFare, _ := bucket.Aggregations.Avg("average_fare")
			totalPassengers, _ := bucket.Aggregations.Sum("total_passengers")
			if avgFare.Value != nil {
				af.AvgFare = cast.ToFloat64(*avgFare.Value)
			} else {
				af.AvgFare = 0 // 设置默认值为 0
			}
			if totalPassengers.Value != nil {
				af.Passengers = cast.ToInt(*totalPassengers.Value)
			} else {
				af.Passengers = 0 // 设置默认值为 0
			}

			topHits, _ := bucket.Aggregations.TopHits("route_info")

			if topHits == nil || topHits.Hits.TotalHits.Value <= 0 || len(topHits.Hits.Hits) <= 0 {
				fmt.Println("No hits found for this bucket.origin:", af.OriginAirport, "dest:", af.DestAirport)
				continue
			}
			hit := topHits.Hits.Hits[0]

			var source map[string]interface{}
			_ = json.Unmarshal(hit.Source, &source)
			af.OriginAirportName = airportMap[af.OriginAirport]
			af.OriginCityName = cityMap[cast.ToString(source["origin_city_market_id"])]
			af.OriginState = cast.ToString(source["origin_state"])
			af.OriginStateName = cast.ToString(source["origin_state_name"])
			af.OriginCountry = cast.ToString(source["origin_country"])

			af.DestAirportName = airportMap[af.DestAirport]
			af.DestCityName = cityMap[cast.ToString(source["dest_city_market_id"])]
			af.DestState = cast.ToString(source["dest_state"])
			af.DestStateName = cast.ToString(source["dest_state_name"])
			af.DestCountry = cast.ToString(source["dest_country"])

			req := elastic.NewBulkIndexRequest().Index(airport_flights_index_name).Id(strings.Join([]string{cast.ToString(af.Year), cast.ToString(af.Quarter), af.OriginAirport, af.DestAirport}, "_")).Doc(af)

			w.Add(req)
		}
		afterKey = agg.AfterKey
		if agg.AfterKey == nil {
			break
		}
	}
	fmt.Println("allcount:", count)
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

// 获取下载数据配置
func getDataConfig() []DateArg {
	var config = []DateArg{}
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

/*
	按照 机场维度 ，把城市和州也写进去,每个季度的平均票价，游客人数，航班数量

先聚合查询每个季度的所有航班信息和航班数量，并且写入数据库。
再逐条查询每个航班的平均票价和游客数量。
*/
func queryAirportFlightByYearQuarter(year, quarter int) {
	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", year),
			elastic.NewTermQuery("quarter", quarter),
		)
	// 定义复合聚合查询
	compositeAgg := elastic.NewCompositeAggregation().Size(10000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("origin").Field("origin"),
		elastic.NewCompositeAggregationTermsValuesSource("dest").Field("dest"))
	// 定义子聚合（top_hits用于获取origin_country和origin_state）
	topHitsAgg := elastic.NewTopHitsAggregation().
		Size(1). // 只需要返回1条记录
		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("origin_city_market_id", "origin_state", "origin_state_name", "origin_country", "dest_city_market_id", "dest_state", "dest_state_name", "dest_country"))

	var afterKey map[string]interface{}
	//var count = 0
	ctx := context.Background()
	w, err := client.BulkProcessor().
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

	for {
		// 每次查询时将 afterKey 传递给聚合
		if afterKey != nil {
			compositeAgg = compositeAgg.AggregateAfter(afterKey)
		}
		// 组合查询
		searchResult, err := client.Search().
			Index(market_index_name). // 索引名称
			Query(boolQuery).         // 添加查询条件
			Size(0).                  // 我们不需要返回文档，设置为0
			Aggregation("unique_routes", compositeAgg.SubAggregation("route_info", topHitsAgg)).
			Do(ctx)
		if err != nil {
			panic(err)
		}

		// 处理结果
		agg, _ := searchResult.Aggregations.Composite("unique_routes")
		for _, bucket := range agg.Buckets {
			//count++
			af := &AirportFlight{}
			af.Year = year
			af.Quarter = quarter
			af.OriginAirport = cast.ToString(bucket.Key["origin"])
			af.DestAirport = cast.ToString(bucket.Key["dest"])
			//af.FlightNum = cast.ToInt(bucket.DocCount)

			topHits, _ := bucket.Aggregations.TopHits("route_info")
			hit := topHits.Hits.Hits[0]

			var source map[string]interface{}
			_ = json.Unmarshal(hit.Source, &source)
			af.OriginAirportName = airportMap[af.OriginAirport]
			af.OriginCityName = cityMap[cast.ToString(source["origin_city_market_id"])]
			af.OriginState = cast.ToString(source["origin_state"])
			af.OriginStateName = cast.ToString(source["origin_state_name"])
			af.OriginCountry = cast.ToString(source["origin_country"])

			af.DestAirportName = airportMap[af.DestAirport]
			af.DestCityName = cityMap[cast.ToString(source["dest_city_market_id"])]
			af.DestState = cast.ToString(source["dest_state"])
			af.DestStateName = cast.ToString(source["dest_state_name"])
			af.DestCountry = cast.ToString(source["dest_country"])

			req := elastic.NewBulkIndexRequest().Index(airport_flights_index_name).Id(strings.Join([]string{cast.ToString(af.Year), cast.ToString(af.Quarter), af.OriginAirport, af.DestAirport}, "_")).Doc(af)

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
	//fmt.Println("最后数量", count)
}

// 更新所有AirportFlight 数据
func updateAirportFlightAvgFareAndSumPassengers(year, quarter int) {

	ctx := context.Background()

	// 初始化 BulkProcessor
	bulkProcessor, err := client.BulkProcessor().
		BulkActions(bulkActions).
		FlushInterval(time.Second).
		Workers(actualNumCPU).
		Stats(true).
		After(GetFailed).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	bulkProcessor.Start(ctx)
	defer bulkProcessor.Close()

	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", year),
			elastic.NewTermQuery("quarter", quarter),
		)

	// 查询 airport_flights 索引以获取所有文档
	scroll, err := client.Scroll().
		Index(airport_flights_index_name).
		Query(boolQuery). // 添加查询条件
		Scroll("10s").
		Size(1000).
		Do(ctx)
	if err != nil {
		panic(err)
	}

	scrollID := scroll.ScrollId

	fmt.Println("total=", scroll.TotalHits())
	for {
		//if scroll != nil {
		//	fmt.Println("scroll.Hits.Hits:", len(scroll.Hits.Hits))
		//}

		//fmt.Println("scroll.Hits.Hits=", len(scroll.Hits.Hits))
		if len(scroll.Hits.Hits) <= 0 {
			fmt.Println("空")
			break
		}
		//s := time.Now().Unix()
		for _, hit := range scroll.Hits.Hits {
			var af AirportFlight
			err := json.Unmarshal(hit.Source, &af)
			if err != nil {
				log.Fatalf("Error unmarshalling hit source: %v", err)
				panic(err)
			}

			// 使用聚合查询从 market 索引中计算平均票价和乘客总数
			marketQuery := elastic.NewBoolQuery().
				Must(
					elastic.NewTermQuery("origin", af.OriginAirport),
					elastic.NewTermQuery("dest", af.DestAirport),
					elastic.NewTermQuery("year", af.Year),
					elastic.NewTermQuery("quarter", af.Quarter),
				)
			avgFareAgg := elastic.NewAvgAggregation().Field("mkt_fare")
			passengersAgg := elastic.NewSumAggregation().Field("passengers")
			searchResult, err := client.Search().
				Index(market_index_name).
				Query(marketQuery).
				Size(0).
				Aggregation("average_fare", avgFareAgg).
				Aggregation("total_passengers", passengersAgg).
				Do(ctx)
			if err != nil {
				log.Fatalf("Error executing market aggregation query: %v", err)
				panic(err)
			}

			avgFare, found := searchResult.Aggregations.Avg("average_fare")
			if !found {
				fmt.Println("avgFare", *avgFare, "value", cast.ToFloat64(*avgFare.Value))
			}
			totalPassengers, found2 := searchResult.Aggregations.Sum("total_passengers")
			if !found2 {
				fmt.Println("totalPassengers", *totalPassengers, "value", cast.ToInt(*totalPassengers.Value))
			}
			//fmt.Println("avgFare", *avgFare.Value, "totalPassengers", *totalPassengers.Value)
			// 更新 airport_flights 索引中的数据
			updateReq := elastic.NewBulkUpdateRequest().
				Index(airport_flights_index_name).
				Id(hit.Id).
				Doc(map[string]interface{}{
					"avg_fare":   cast.ToFloat64(*avgFare.Value),
					"passengers": cast.ToInt(*totalPassengers.Value),
				}).DocAsUpsert(false)

			bulkProcessor.Add(updateReq)
		}
		//scroll, err = client.Scroll("10s").ScrollId(scrollID).Do(ctx)
		//fmt.Println("处理当前批时间", time.Now().Unix()-s, "s")

		scroll, err = client.Scroll("10s").ScrollId(scrollID).Do(ctx)
		if err != nil {
			if err == io.EOF || err.Error() == "EOF" {
				fmt.Println(year, quarter, "查询数据空,结束")
				break
			}
			fmt.Println("Scroll,err", err)
			break
		}

		//if err != nil {
		//	if err.Error() == "EOF" {
		//		fmt.Println("error EOF")
		//		break
		//	}
		//	fmt.Println("255err", err)
		//	break
		//}
		//if scroll == nil {
		//	fmt.Println("scroll空")
		//	break
		//}
		//if len(scroll.Hits.Hits) <= 0 {
		//	fmt.Println("Hits空")
		//	break
		//}

		//scrollID = scroll.ScrollId
	}
	// 清除游标
	//if scroll != nil {
	_, err = client.ClearScroll().ScrollId(scroll.ScrollId).Do(ctx)

	//}

	for {
		st1 := bulkProcessor.Stats() //获取数据写入情况
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
	st := bulkProcessor.Stats() //获取数据写入情况
	log.Printf("bulkProcessor state Succeeded:%d Failed:%d Created:%d Updated:%d Deleted:%d Flushed:%d Committed:%d Indexed:%d\n", st.Succeeded, st.Failed, st.Created, st.Updated, st.Deleted, st.Flushed, st.Committed, st.Indexed)

	//fmt.Println("最后航班数量", count)
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

// 读取城市表到内存
func readCityMarketID() {

	f, e := os.Open("L_CITY_MARKET_ID.csv")
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
			arr := strings.Split(record[1], ":")
			cityMap[record[0]] = arr[0]
		}
	}
	fmt.Println("读取城市完成:", n)
}

func connectEs() {
	var err error
	client, err = elastic.NewClient(
		// 设置ES服务地址，支持多个地址
		elastic.SetURL("http://127.0.0.1:9200/"),
		elastic.SetSniff(false))
	if err != nil {
		// Handle error
		fmt.Printf("连接失败: %v\n", err)
	} else {
		fmt.Println("连接成功")

	}

}
func initFlightsIndex() {
	ctx := context.Background()
	exists, err := client.IndexExists(airport_flights_index_name).Do(ctx)
	if err != nil {
		fmt.Println("判断airport_flights_index_name是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		fmt.Println(airport_flights_index_name, "索引已存在")
		return
	}
	mapping := `{
    "mappings": {
        "properties": {
            "year": {
                "type": "integer"
            },
            "quarter": {
                "type": "short"
            },
            "origin_airport": {
                "type": "keyword"
            },
            "origin_airport_name": {
                "type": "keyword"
            },
            "origin_city_name": {
                "type": "keyword"
            },
            "origin_state": {
                "type": "keyword"
            },
            "origin_state_name": {
                "type": "keyword"
            },
            "origin_country": {
                "type": "keyword"
            },

            "dest_airport": {
                "type": "keyword"
            },
            "dest_airport_name": {
                "type": "keyword"
            },
            "dest_city_name": {
                "type": "keyword"
            },
            "dest_state": {
                "type": "keyword"
            },
            "dest_state_name": {
                "type": "keyword"
            },
            "dest_country": {
                "type": "keyword"
            },
            "passengers": {
                "type": "integer"
            },
            "avg_fare": {
                "type": "scaled_float",
                "scaling_factor": 100
            },
            "flight_num": {
                "type": "integer"
            }
        }
    }
}`
	index, err := client.CreateIndex(airport_flights_index_name).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建airport_flights_index_name失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建airport_flights_index_name.Acknowledged.no")
		os.Exit(0)
	}
	fmt.Println("initairport_flights_index_name成功")
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

type AirportFlight struct {
	Year    int `json:"year"`    //年
	Quarter int `json:"quarter"` //季度

	OriginAirport     string `json:"origin_airport"`      //出发地机场代码
	OriginAirportName string `json:"origin_airport_name"` //出发地机场名称
	OriginCityName    string `json:"origin_city_name"`    //出发地城市名称
	OriginState       string `json:"origin_state"`        //出发地州代码
	OriginStateName   string `json:"origin_state_name"`   //出发地州名称
	OriginCountry     string `json:"origin_country"`      //出发地国家代码

	DestAirport     string `json:"dest_airport"`      //目的地机场代码
	DestAirportName string `json:"dest_airport_name"` //目的地机场名称
	DestCityName    string `json:"dest_city_name"`    //目的地城市名称
	DestState       string `json:"dest_state"`        //目的地州代码
	DestStateName   string `json:"dest_state_name"`   //目的地州名称
	DestCountry     string `json:"dest_country"`      //目的地国家代码

	Passengers int     `json:"passengers"` // 乘客数量
	AvgFare    float64 `json:"avg_fare"`   // 平均市场票价
}
