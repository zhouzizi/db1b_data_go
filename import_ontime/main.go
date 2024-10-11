package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	ESUrl               = "http://127.0.0.1:9200/"
	bulkActions         = 1000
	DownloadUrl         = "https://transtats.bts.gov/PREZIP/"
	NamePrefix          = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"
	CVSNamePrefix       = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_"
	DefaultGoroutineNum = 5
	TempZipFolderPath   = "temp_zips/"
	TempCsvFolderPath   = "temp_csvs/"
	OnTimeDataIndexName = "on_time_data"
)

type Date struct {
	Year  int
	Month int
	Down  int
	Unzip int
}

var (
	actualNumCPU = runtime.GOMAXPROCS(0)
	esClient     *elastic.Client
	dates        = []Date{}
	nowErr       = false
)

func main() {
	//连接es
	connectES()
	createIndex()
	err := createTempFolder()
	if err != nil {
		os.Exit(0)
	}
	dates = getDateConfig()
	if dates == nil {
		os.Exit(0)
	}
	fmt.Println("待下载数据时间为:", dates)

	goroutineNum := initGoroutineNum()
	if goroutineNum > len(dates) {
		goroutineNum = len(dates)
	}
	fmt.Println("下载线程数为：", goroutineNum)
	fmt.Println("导入线程数为：", actualNumCPU)

	fmt.Println("--------start")
	start := time.Now().Unix()
	semaphore := make(chan struct{}, goroutineNum)
	var wg sync.WaitGroup
	for i := range dates {
		wg.Add(1)
		go func(i int) {
			d := dates[i]
			defer wg.Done()
			semaphore <- struct{}{}
			err := downloadFile(d.Year, d.Month)
			if err != nil {
				fmt.Println("【下载】", d.Year, "年", d.Month, "月文件失败")
			} else {
				dates[i].Down = 1
			}
			<-semaphore
		}(i)
	}
	wg.Wait()
	fmt.Println("下载结束，耗时", time.Now().Unix()-start, "s")

	fmt.Println("开始解压")
	start = time.Now().Unix()
	//读取临时目录下所有文件进行解压
	unzipAllFiles()
	fmt.Println("解压文件完成，耗时", time.Now().Unix()-start, "s")

	//开始导入到ES
	for i := range dates {
		d := dates[i]
		if d.Unzip != 1 {
			continue
		}
		suc := importData(d.Year, d.Month)
		if !suc {
			fmt.Println("【导入】", d.Year, "年", d.Month, "月文件失败")
		}
	}
	fmt.Println("总耗时", time.Now().Unix()-start, "s")
	fmt.Println("--------over")

}

// 从参数读取线程数
func initGoroutineNum() int {
	args := os.Args
	if len(args) > 1 {
		num := cast.ToInt(args[1])
		if num < 1 {
			fmt.Println("线程数异常：", num)
			return DefaultGoroutineNum
		} else {
			return num
		}
	} else {
		fmt.Println("未输入线程数")
		return DefaultGoroutineNum

	}
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

// 创建临时文件夹
func createTempFolder() error {
	// 使用Mkdir创建文件夹
	err := os.Mkdir(TempZipFolderPath, os.ModePerm)
	if err != nil {
		// 如果文件夹已存在，也可以选择忽略错误
		if !os.IsExist(err) {
			fmt.Println("Error creating directory1:", err)
			return err
		}
	}
	err2 := os.Mkdir(TempCsvFolderPath, os.ModePerm)
	if err2 != nil {
		// 如果文件夹已存在，也可以选择忽略错误
		if !os.IsExist(err2) {
			fmt.Println("Error creating directory2:", err2)
			return err2
		}
	}
	return nil
}

// 根据年份季度下载market数据
func downloadFile(year, month int) error {
	fileName := fmt.Sprintf("%s%d_%d.zip", NamePrefix, year, month)
	_, err := os.Stat(TempZipFolderPath + fileName)
	if err == nil {
		fmt.Println(fileName, "下载过")
		return nil
	}
	url := DownloadUrl + fileName
	//fmt.Println("下载文件地址:", url)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(fileName, "下载错误", err)
		return err
	}
	defer resp.Body.Close()
	//os.IsExist()
	file, err := os.OpenFile(TempZipFolderPath+fileName, os.O_RDWR|os.O_CREATE, os.ModePerm)
	//file, err := os.Create("temp_zip/" + fileName + ".zip")
	if err != nil {
		fmt.Println(fileName, "创建文件错误", err)
		return err
	}
	defer file.Close()
	size, err := io.Copy(file, resp.Body)
	if err != nil {
		fmt.Println(fileName, "保存文件错误", err)
		return err
	}
	fmt.Println(fileName, "文件大小", size)

	return nil
}

// 解压文件
func unzipAllFiles() {
	for i := range dates {
		d := dates[i]
		if d.Down != 1 {
			continue
		}
		err := unzipFile(d.Year, d.Month)
		if err != nil {
			fmt.Println("【解压】", d.Year, "年", d.Month, "月文件失败")
		} else {
			dates[i].Unzip = 1
		}
	}
}
func unzipFile(year, month int) error {
	csvFileName := fmt.Sprintf("%s%d_%d.csv", CVSNamePrefix, year, month)
	_, err := os.Stat(TempCsvFolderPath + csvFileName)
	if err == nil {
		fmt.Println(csvFileName, "已解压")
		return nil
	}
	zipFileName := fmt.Sprintf("%s%s%d_%d.zip", TempZipFolderPath, NamePrefix, year, month)
	archive, err := zip.OpenReader(zipFileName)
	if err != nil {
		fmt.Println("打开压缩包", zipFileName, "失败:", err)
		return err
	}
	defer archive.Close()
	for _, f := range archive.File {
		fmt.Println("filename:", f.Name)
		if strings.HasSuffix(f.Name, ".csv") {
			dest, err := os.Create(TempCsvFolderPath + f.Name)
			if err != nil {
				fmt.Println("创建", f.Name, "失败:", err)
				return err
			}
			defer dest.Close()
			src, err := f.Open()
			if err != nil {
				fmt.Println("打开", f.Name, "失败:", err)
				return err
			}
			defer src.Close()
			_, err = io.Copy(dest, src)
			if err != nil {
				fmt.Println("保存", f.Name, "失败:", err)
				return err
			}

		}
	}
	return nil
}

func importData(year, month int) bool {
	//清空已有数据防止重复
	clearSuc := clearData(year, month)
	if !clearSuc {
		return false
	}
	suc := readCsv(year, month)
	if !suc {
		return false
	}
	return true
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
func createIndex() {
	ctx := context.Background()
	exists, err := esClient.IndexExists(OnTimeDataIndexName).Do(ctx)
	if err != nil {
		fmt.Println("判断index是否存在失败:", err)
		os.Exit(0)
	}
	if exists {
		// Index does not exist yet.
		fmt.Println(OnTimeDataIndexName, "索引已存在")
		return
	}
	mapping := `{
    "mappings": {
        "properties": {
            "year": {
                "type": "short"
            },
            "quarter": {
                "type": "short"
            },
            "month": {
                "type": "short"
            },
            "dayofmonth": {
                "type": "short"
            },
            "dayofweek": {
                "type": "short"
            },
            "flight_date": {
                "type": "keyword"
            },
            "reporting_airline": {
                "type": "keyword"
            },
            "dot_id_reporting_airline": {
                "type": "keyword"
            },
            "iata_code_reporting_airline": {
                "type": "keyword"
            },
            "tail_number": {
                "type": "keyword"
            },
            "flight_number_reporting_airline": {
                "type": "keyword"
            },
            "origin_airport_id": {
                "type": "keyword"
            },
            "origin_airport_seq_id": {
                "type": "keyword"
            },
            "origin_city_market_id": {
                "type": "keyword"
            },
            "origin": {
                "type": "keyword"
            },
            "origin_city_name": {
                "type": "keyword"
            },
            "origin_state": {
                "type": "keyword"
            },
            "origin_state_fips": {
                "type": "short"
            },
            "origin_state_name": {
                "type": "keyword"
            },
            "origin_wac": {
                "type": "keyword"
            },
            "dest_airport_id": {
                "type": "keyword"
            },
            "dest_airport_seq_id": {
                "type": "keyword"
            },
            "dest_city_market_id": {
                "type": "keyword"
            },
            "dest": {
                "type": "keyword"
            },
            "dest_city_name": {
                "type": "keyword"
            },
            "dest_state": {
                "type": "keyword"
            },
            "dest_state_fips": {
                "type": "short"
            },
            "dest_state_name": {
                "type": "text"
            },
            "dest_wac": {
                "type": "short"
            },
            "crs_dep_time": {
                "type": "integer"
            },
            "dep_time": {
                "type": "integer"
            },
            "dep_delay": {
                "type": "integer"
            },
            "dep_delay_minutes": {
                "type": "integer"
            },
            "dep_del15": {
                "type": "integer"
            },
            "departure_delay_groups": {
                "type": "integer"
            },
            "dep_time_blk": {
                "type": "keyword"
            },
            "taxi_out": {
                "type": "integer"
            },
            "wheels_off": {
                "type": "integer"
            },
            "wheels_on": {
                "type": "integer"
            },
            "taxi_in": {
                "type": "integer"
            },
            "crs_arr_time": {
                "type": "integer"
            },
            "arr_time": {
                "type": "integer"
            },
            "arr_delay": {
                "type": "integer"
            },
            "arr_delay_minutes": {
                "type": "integer"
            },
            "arr_del15": {
                "type": "integer"
            },
            "arrival_delay_groups": {
                "type": "integer"
            },
            "arr_time_blk": {
                "type": "keyword"
            },
            "cancelled": {
                "type": "short"
            },
            "cancellation_code": {
                "type": "keyword"
            },
            "diverted": {
                "type": "short"
            },
            "crs_elapsed_time": {
                "type": "integer"
            },
            "actual_elapsed_time": {
                "type": "integer"
            },
            "air_time": {
                "type": "integer"
            },
            "flights": {
                "type": "short"
            },
            "distance": {
                "type": "scaled_float",
                "scaling_factor": 100
            },
            "distance_group": {
                "type": "short"
            }
        }
    }
}`
	index, err := esClient.CreateIndex(OnTimeDataIndexName).BodyString(mapping).Do(ctx)
	if err != nil {
		fmt.Println("创建index失败:", err)
		os.Exit(0)
	}
	if !index.Acknowledged {
		// Not acknowledged
		fmt.Println("创建index.Acknowledged.no")
		os.Exit(0)
	}
	fmt.Println("创建index成功")
}

// 清空指定年份季度数据
func clearData(year, month int) bool {
	ctx := context.Background()
	res, err := esClient.DeleteByQuery(OnTimeDataIndexName).Query(elastic.NewBoolQuery().Must(elastic.NewTermsQuery("year", year), elastic.NewTermsQuery("month", month))).Do(ctx)

	if err != nil {
		// Handle error
		fmt.Println("删除", year, "年", month, "月数据失败:", err)
		return false
	}

	fmt.Println("删除", year, "年", month, "月", res.Total, "条旧数据ok")
	return true
}

// 读取csv文件
func readCsv(year, month int) bool {
	fileName := fmt.Sprintf("%s%d_%d.csv", CVSNamePrefix, year, month)
	f, e := os.Open(TempCsvFolderPath + fileName)
	if e != nil {
		fmt.Println("读取", fileName, "失败:", e)
		return false
	}
	defer f.Close()
	reader := csv.NewReader(f)
	nowErr = false
	ctx := context.Background()
	w, err := esClient.BulkProcessor().
		BulkActions(bulkActions).
		FlushInterval(time.Second).
		Workers(actualNumCPU).
		Stats(true).
		After(GetFailed).
		Do(ctx)
	if err != nil {
		fmt.Println("esClient.BulkProcessor", fileName, "失败:", err)
		return false
	}
	w.Start(ctx)
	defer w.Close()
	//var i = 0
	var n = 0
	for {
		if nowErr {
			break
		}
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("逐行读取", fileName, "失败:", err)
		}
		if record[0] != "Year" {
			d := &OnTimeData{
				Year:                         cast.ToInt(record[0]),
				Quarter:                      cast.ToInt(record[1]),
				Month:                        cast.ToInt(record[2]),
				DayofMonth:                   cast.ToInt(record[3]),
				DayofWeek:                    cast.ToInt(record[4]),
				FlightDate:                   record[5],
				ReportingAirline:             record[6],
				DotIDReportingAirline:        record[7],
				IATACodeReportingAirline:     record[8],
				TailNumber:                   record[9],
				FlightNumberReportingAirline: record[10],
				OriginAirportID:              record[11],
				OriginAirportSeqID:           record[12],
				OriginCityMarketID:           record[13],
				Origin:                       record[14],
				OriginCityName:               record[15],
				OriginState:                  record[16],
				OriginStateFips:              cast.ToInt(record[17]),
				OriginStateName:              record[18],
				OriginWac:                    record[19],
				DestAirportID:                record[20],
				DestAirportSeqID:             cast.ToInt(record[21]),
				DestCityMarketID:             record[22],
				Dest:                         record[23],
				DestCityName:                 record[24],
				DestState:                    record[25],
				DestStateFips:                cast.ToInt(record[26]),
				DestStateName:                record[27],
				DestWac:                      cast.ToInt(record[28]),
				CrsDepTime:                   cast.ToInt(record[29]),
				DepTime:                      cast.ToInt(record[30]),
				DepDelay:                     cast.ToInt(record[31]),
				DepDelayMinutes:              cast.ToInt(record[32]),
				DepDel15:                     cast.ToInt(record[33]),
				DepartureDelayGroups:         cast.ToInt(record[34]),
				DepTimeBlk:                   record[25],
				TaxiOut:                      cast.ToInt(record[36]),
				WheelsOff:                    cast.ToInt(record[37]),
				WheelsOn:                     cast.ToInt(record[38]),
				TaxiIn:                       cast.ToInt(record[39]),
				CrsArrTime:                   cast.ToInt(record[40]),
				ArrTime:                      cast.ToInt(record[41]),
				ArrDelay:                     cast.ToInt(record[42]),
				ArrDelayMinutes:              cast.ToInt(record[43]),
				ArrDel15:                     cast.ToInt(record[44]),
				ArrivalDelayGroups:           cast.ToInt(record[45]),
				ArrTimeBlk:                   record[46],
				Cancelled:                    cast.ToInt(record[47]),
				CancellationCode:             record[48],
				Diverted:                     cast.ToInt(record[49]),
				CrsElapsedTime:               cast.ToInt(record[50]),
				ActualElapsedTime:            cast.ToInt(record[51]),
				AirTime:                      cast.ToInt(record[52]),
				Flights:                      cast.ToInt(record[53]),
				Distance:                     cast.ToFloat64(record[54]),
				DistanceGroup:                cast.ToInt(record[55]),
			}
			req := elastic.NewBulkIndexRequest().Index(OnTimeDataIndexName).Doc(d)
			n++
			w.Add(req)
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

	if nowErr {
		//为保证数据完整性，发现存在错误则清空该季度数据
		fmt.Println(year, "年", month, "月存在导入错误")
		clearData(year, month)
		return false
	}
	time.Sleep(20 * time.Second)
	fmt.Println(year, "年", month, "月总条数:", n)
	queryNum := queryDataNum(year, month)
	fmt.Println("查询数据库条数为:", queryNum)
	if queryNum == int64(n) {
		fmt.Println("【成功】", year, "年", month, "月导入成功")
	} else {
		fmt.Println("【异常】", year, "年", month, "月导入数据不一致")
	}
	return true
}
func queryDataNum(year, month int) int64 {
	ctx := context.Background()
	count, err := esClient.Count(OnTimeDataIndexName).Query(elastic.NewBoolQuery().Must(elastic.NewTermsQuery("year", year), elastic.NewTermsQuery("month", month))).Do(ctx)
	if err != nil {
		fmt.Println("queryDataNum", year, "年", month, "月数据失败:", err)
		return 0
	}
	return count
}
func GetFailed(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response == nil { //可能存在为空的情况 😳
		//log.Println("GetNil response return")
		return
	}
	fi := response.Failed()
	if len(fi) != 0 {
		for _, f := range fi {
			nowErr = true
			log.Printf("DebugFailedEs: index:%s type:%s id:%s version:%d  status:%d result:%s ForceRefresh:%v errorDetail:%v getResult:%v\n", f.Index, f.Type, f.Id, f.Version, f.Status, f.Result, f.ForcedRefresh, f.Error, f.GetResult)
			//panic(f.Error)
		}
	}

}

type OnTimeData struct {
	Year                         int     `json:"year"`
	Quarter                      int     `json:"quarter"`
	Month                        int     `json:"month"`
	DayofMonth                   int     `json:"dayof_month"`
	DayofWeek                    int     `json:"dayof_week"`
	FlightDate                   string  `json:"flight_date"`
	ReportingAirline             string  `json:"reporting_airline"`
	DotIDReportingAirline        string  `json:"dot_id_reporting_airline"`
	IATACodeReportingAirline     string  `json:"iata_code_reporting_airline"`
	TailNumber                   string  `json:"tail_number"`
	FlightNumberReportingAirline string  `json:"flight_number_reporting_airline"`
	OriginAirportID              string  `json:"origin_airport_id"`
	OriginAirportSeqID           string  `json:"origin_airport_seq_id"`
	OriginCityMarketID           string  `json:"origin_city_market_id"`
	Origin                       string  `json:"origin"`
	OriginCityName               string  `json:"origin_city_name"`
	OriginState                  string  `json:"origin_state"`
	OriginStateFips              int     `json:"origin_state_fips"`
	OriginStateName              string  `json:"origin_state_name"`
	OriginWac                    string  `json:"origin_wac"`
	DestAirportID                string  `json:"dest_airport_id"`
	DestAirportSeqID             int     `json:"dest_airport_seq_id"`
	DestCityMarketID             string  `json:"dest_city_market_id"`
	Dest                         string  `json:"dest"`
	DestCityName                 string  `json:"dest_city_name"`
	DestState                    string  `json:"dest_state"`
	DestStateFips                int     `json:"dest_state_fips"`
	DestStateName                string  `json:"dest_state_name"`
	DestWac                      int     `json:"dest_wac"`
	CrsDepTime                   int     `json:"crs_dep_time"`
	DepTime                      int     `json:"dep_time"`
	DepDelay                     int     `json:"dep_delay"`
	DepDelayMinutes              int     `json:"dep_delay_minutes"`
	DepDel15                     int     `json:"dep_del15"`
	DepartureDelayGroups         int     `json:"departure_delay_groups"`
	DepTimeBlk                   string  `json:"dep_time_blk"`
	TaxiOut                      int     `json:"taxi_out"`
	WheelsOff                    int     `json:"wheels_off"`
	WheelsOn                     int     `json:"wheels_on"`
	TaxiIn                       int     `json:"taxi_in"`
	CrsArrTime                   int     `json:"crs_arr_time"`
	ArrTime                      int     `json:"arr_time"`
	ArrDelay                     int     `json:"arr_delay"`
	ArrDelayMinutes              int     `json:"arr_delay_minutes"`
	ArrDel15                     int     `json:"arr_del15"`
	ArrivalDelayGroups           int     `json:"arrival_delay_groups"`
	ArrTimeBlk                   string  `json:"arr_time_blk"`
	Cancelled                    int     `json:"cancelled"`
	CancellationCode             string  `json:"cancellation_code"`
	Diverted                     int     `json:"diverted"`
	CrsElapsedTime               int     `json:"crs_elapsed_time"`
	ActualElapsedTime            int     `json:"actual_elapsed_time"`
	AirTime                      int     `json:"air_time"`
	Flights                      int     `json:"flights"`
	Distance                     float64 `json:"distance"`
	DistanceGroup                int     `json:"distance_group"`
}
