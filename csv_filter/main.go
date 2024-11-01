package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type Condition struct {
	Field string      `json:"field"`
	Op    string      `json:"op"`
	Value interface{} `json:"value"`
}

type Config struct {
	Conditions []Condition `json:"conditions"`
	FileName   string      `json:"file_name"`
}

// loadConfig 从 JSON 文件加载筛选配置
func loadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// matchesCondition 检查给定值是否符合条件
func matchesCondition(value string, condition Condition) bool {
	switch condition.Op {
	case "equals":
		return value == condition.Value.(string)
	case "not_equals":
		return value != condition.Value.(string)
	case "in":
		values := condition.Value.([]interface{})
		for _, v := range values {
			if value == v.(string) {
				return true
			}
		}
		return false
	case "range":
		rangeValues := condition.Value.([]interface{})
		lower, _ := strconv.ParseFloat(rangeValues[0].(string), 64)
		upper, _ := strconv.ParseFloat(rangeValues[1].(string), 64)
		val, _ := strconv.ParseFloat(value, 64)
		return val >= lower && val <= upper
	default:
		return false
	}
}

// filterCSV 读取 CSV 文件并根据配置筛选记录
func filterCSV(config *Config) error {
	file, err := os.Open(config.FileName)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		return err
	}

	headerIndex := make(map[string]int)
	for i, header := range headers {
		headerIndex[header] = i
	}

	for _, condition := range config.Conditions {
		if _, ok := headerIndex[condition.Field]; !ok {
			return fmt.Errorf("列 %s 在 CSV 文件中未找到", condition.Field)
		}
	}

	outputFile, err := os.Create("filtered_" + config.FileName)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	if err := writer.Write(headers); err != nil {
		return err
	}
	matchCount := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		match := true
		for _, condition := range config.Conditions {
			index := headerIndex[condition.Field]
			if !matchesCondition(record[index], condition) {
				match = false
				break
			}
		}

		if match {
			if err := writer.Write(record); err != nil {
				return err
			}
			matchCount++
		}
	}
	fmt.Printf("匹配的记录总数: %d\n", matchCount)
	return nil
}

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}

	if err := filterCSV(config); err != nil {
		log.Fatalf("筛选 CSV 文件失败: %v", err)
	}

	fmt.Println("筛选完成，结果已保存到新文件。")
}
