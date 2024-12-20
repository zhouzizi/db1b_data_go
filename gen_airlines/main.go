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
	cityInfoMap  = map[string]bool{} //key：city_market_id ,value:domestic
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

	initAirlinesIndex()
	fmt.Println(time.Now().String(), "=====start")
	start := time.Now().Unix()
	for _, d := range config.Dates {
		queryAirlines(d)
	}
	fmt.Println(time.Now().String(), "=====end")
	fmt.Println("航班信息添加总耗时", time.Now().Unix()-start, "s")
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
	if count == 0 {
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
      "year": {
        "type": "short"
      },
      "month": {
        "type": "short"
      },
      "air_carrier": {
        "type": "keyword"
      },
      "flight_number": {
        "type": "keyword"
      },
      "origin_airport": {
        "type": "keyword"
      },
      "origin_city": {
        "type": "keyword"
      },
      "origin_state": {
        "type": "keyword"
      },
      "dest_airport": {
        "type": "keyword"
      },
      "dest_city": {
        "type": "keyword"
      },
      "dest_state": {
        "type": "keyword"
      },
      "domestic": {
        "type": "boolean"
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

func queryAirlines(d Date) {
	boolQuery := elastic.NewBoolQuery().
		Must(
			elastic.NewTermQuery("year", d.Year),
			elastic.NewTermQuery("month", d.Month),
		)
	// 定义复合聚合查询
	compositeAgg := elastic.NewCompositeAggregation().Size(2000).Sources(
		elastic.NewCompositeAggregationTermsValuesSource("year").Field("year"),
		elastic.NewCompositeAggregationTermsValuesSource("month").Field("month"),
		elastic.NewCompositeAggregationTermsValuesSource("origin").Field("origin"),
		elastic.NewCompositeAggregationTermsValuesSource("dest").Field("dest"),
		elastic.NewCompositeAggregationTermsValuesSource("iata_code_reporting_airline").Field("iata_code_reporting_airline"),
		elastic.NewCompositeAggregationTermsValuesSource("flight_number_reporting_airline").Field("flight_number_reporting_airline"),
	)
	// 定义子聚合（top_hits用于获取origin_country和origin_state）
	topHitsAgg := elastic.NewTopHitsAggregation().
		Size(1). // 只需要返回1条记录
		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("origin_city_name", "dest_city_name", "origin_city_market_id", "dest_city_market_id"))

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
			al.Year = cast.ToInt(bucket.Key["year"])
			al.Month = cast.ToInt(bucket.Key["month"])
			al.AirCarrier = cast.ToString(bucket.Key["iata_code_reporting_airline"])
			al.OriginAirport = cast.ToString(bucket.Key["origin"])
			al.DestAirport = cast.ToString(bucket.Key["dest"])
			al.FlightNumber = al.AirCarrier + cast.ToString(bucket.Key["flight_number_reporting_airline"])

			topHits, _ := bucket.Aggregations.TopHits("route_info")
			hit := topHits.Hits.Hits[0]

			var source map[string]interface{}
			_ = json.Unmarshal(hit.Source, &source)

			origin_city_name := cast.ToString(source["origin_city_name"])
			al.OriginCity = strings.Split(origin_city_name, ", ")[0]
			al.OriginState = strings.Split(origin_city_name, ", ")[1]
			dest_city_name := cast.ToString(source["dest_city_name"])
			al.DestCity = strings.Split(dest_city_name, ", ")[0]
			al.DestState = strings.Split(dest_city_name, ", ")[1]

			origin_city_market_id := cast.ToString(source["origin_city_market_id"])
			dest_city_market_id := cast.ToString(source["dest_city_market_id"])
			originDomestic, _ := cityInfoMap[origin_city_market_id]
			destDomestic, _ := cityInfoMap[dest_city_market_id]
			if originDomestic && destDomestic {
				al.Domestic = true
			} else {
				al.Domestic = false
			}

			req := elastic.NewBulkIndexRequest().Index(AirlinesIndexName).Id(strings.Join([]string{cast.ToString(bucket.Key["year"]), cast.ToString(bucket.Key["month"]), al.OriginAirport, al.DestAirport, al.AirCarrier, cast.ToString(bucket.Key["flight_number_reporting_airline"])}, "_")).Doc(al)
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
	fmt.Println(d.Year, d.Month, "最后数量", count)
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
	Year          int    `json:"year"`
	Month         int    `json:"month"`
	AirCarrier    string `json:"air_carrier"`
	FlightNumber  string `json:"flight_number"`
	OriginAirport string `json:"origin_airport"`
	OriginCity    string `json:"origin_city"`
	OriginState   string `json:"origin_state"`
	DestAirport   string `json:"dest_airport"`
	DestCity      string `json:"dest_city"`
	DestState     string `json:"dest_state"`
	Domestic      bool   `json:"domestic"`
}
type CityInfo struct {
	Name     string `json:"name"`
	State    string `json:"state"`
	Code     string `json:"code"`
	Domestic bool   `json:"domestic"`
}
