package main

import (
	"encoding/binary"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	jjson "github.com/chaolihf/udpgo/json"
	"github.com/go-kit/log"
)

var logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

type Collector struct {
	listenUdpPort int
	listenIp      string
	kafkaInfo     string
	kafkaTopic    string
	jetLag        int
	headLength    int
	messageLength int
	messageMap    map[string]string
	tableDataMap  map[string]int32
	tableName     string
}

/*
读取连接信息配置文件
*/
func newNetCollector() (*Collector, error) {
	filePath := "config.json"
	content, err := os.ReadFile(filePath)
	if err != nil {
		logger.Log("读取文件出错:"+filePath, err)
	} else {
		jsonConfigInfos, err := jjson.NewJsonObject([]byte(content))
		if err != nil {
			logger.Log("JSON文件格式出错:", err)
			return nil, err
		} else {
			messageMap := make(map[string]string)
			tableDataMap := make(map[string]int32)
			messageKeys := jsonConfigInfos.GetJsonObject("netFlowMap")
			keysInfo := messageKeys.GetKeys()
			for i := range keysInfo {
				messageMap[keysInfo[i]] = messageKeys.GetString(keysInfo[i])
			}
			tableDataKeys := jsonConfigInfos.GetJsonObject("tableData")
			tableDataKeysInfo := tableDataKeys.GetKeys()
			for i := range tableDataKeysInfo {
				tableDataMap[tableDataKeysInfo[i]] = int32(tableDataKeys.GetInt(tableDataKeysInfo[i]))
			}
			return &Collector{
				listenUdpPort: jsonConfigInfos.GetInt("listenUdpPort"),
				listenIp:      jsonConfigInfos.GetString("listenIp"),
				kafkaInfo:     jsonConfigInfos.GetString("kafkaInfo"),
				kafkaTopic:    jsonConfigInfos.GetString("kafkaTopic"),
				jetLag:        jsonConfigInfos.GetInt("jetLag"),
				headLength:    jsonConfigInfos.GetInt("headLength"),
				messageLength: jsonConfigInfos.GetInt("messageLength"),
				messageMap:    messageMap,
				tableDataMap:  tableDataMap,
				tableName:     jsonConfigInfos.GetString("tableName"),
			}, nil
		}
	}
	return &Collector{
		listenUdpPort: 2055,
		listenIp:      "0.0.0.0",
		kafkaInfo:     "127.0.0.1:9092",
		kafkaTopic:    "netflow-log",
		jetLag:        0,
		headLength:    16,
		messageLength: 64,
		messageMap:    nil,
		tableDataMap:  nil,
		tableName:     "",
	}, nil
}

/*
定义数据类型解析规则
*/
func parseRule(dataType string, start int, end int, buffer []byte, jetLag int) interface{} {
	switch dataType {
	case "DATE":
		if int64(binary.BigEndian.Uint32(buffer[start:end])) == 0 {
			return ""
		} else {
			return time.Unix(int64(binary.BigEndian.Uint32(buffer[start:end])), 0).Add(time.Duration(jetLag) * time.Hour).Format("2006-01-02 15:04:05")
		}
	case "IP":
		return net.IP(buffer[start:end]).String()
	case "USHORT2":
		return int(binary.BigEndian.Uint16(buffer[start:end]))
	case "ULONG4":
		return int(binary.BigEndian.Uint32(buffer[start:end]))
	default:
		return nil
	}
}

/*
字符串value值拆分
*/
func stringSplit(value string) (int, int, string, int) {
	valueParts := strings.Split(value, ":")
	start, err := strconv.Atoi(valueParts[0])
	if err != nil {
		logger.Log("解析起始位未配置")
	}
	end, err := strconv.Atoi(valueParts[1])
	if err != nil {
		logger.Log("解析结束位未配置")
	}
	dataType := valueParts[2]
	location, err := strconv.Atoi(valueParts[3])
	if err != nil {
		logger.Log("数据位未配置")
	}
	return start, end, dataType, location
}

/*
将数据转化为protobuf格式
int32 i=1;
int64 l=2;
double d=3;
bool b=4;
string s=5;
timestamp 6
*/
func createTable(collector *Collector) *TableData {
	tableDataMap := collector.tableDataMap
	tableName := collector.tableName
	collectTableData := &TableData{}
	collectTableData.TableName = tableName
	for key, value := range tableDataMap {
		collectTableData.Columns = append(collectTableData.Columns,
			&ColumnData{ColumnName: key, ColumnType: value})
	}
	return collectTableData
}

/*
字段数据添加
*/
func addTableData(collector *Collector, kafkaMessageKeyValues map[string]interface{}, tableData *TableData) {
	tableDataMap := collector.tableDataMap
	tableOneRow := &RowValue{}
	for key, value := range tableDataMap {
		tableData := kafkaMessageKeyValues[key]
		switch value {
		case 1:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_I{I: tableData.(int32)}})
		case 2:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_L{L: tableData.(int64)}})
		case 3:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_D{D: tableData.(float64)}})
		case 4:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_B{B: tableData.(bool)}})
		case 5:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_S{S: tableData.(string)}})
		case 6:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_L{L: tableData.(int64)}})
		}
	}
	tableData.Rows = append(tableData.Rows, tableOneRow)
}
