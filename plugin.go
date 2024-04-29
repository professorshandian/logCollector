package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	jjson "github.com/chaolihf/udpgo/json"
	"github.com/go-kit/log"
)

var logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

type Collector struct {
	listenUdpPort     int
	listenIp          string
	kafkaInfo         string
	kafkaTopic        string
	jetLag            int
	headLength        int
	messageLength     int
	messageMap        map[string]string
	tableDataMap      map[string]int32
	tableName         string
	columnDataCollect []string
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
			columnOrderMap := make(map[string]string)
			columnOrderKeys := jsonConfigInfos.GetJsonObject("columnOrder")
			columnOrderKeysInfo := columnOrderKeys.GetKeys()
			for i := range columnOrderKeysInfo {
				columnOrderMap[columnOrderKeysInfo[i]] = columnOrderKeys.GetString(columnOrderKeysInfo[i])
			}
			tableDataKeysInfo := []string{}
			for i := 0; i < len(columnOrderMap); i++ {
				str := strconv.Itoa(i)
				tableDataKeysInfo = append(tableDataKeysInfo, columnOrderMap[str])
			}
			messageKeys := jsonConfigInfos.GetJsonObject("netFlowMap")
			keysInfo := messageKeys.GetKeys()
			for i := range keysInfo {
				messageMap[keysInfo[i]] = messageKeys.GetString(keysInfo[i])
			}
			tableDataKeys := jsonConfigInfos.GetJsonObject("tableData")
			for i := range tableDataKeysInfo {
				tableDataMap[tableDataKeysInfo[i]] = int32(tableDataKeys.GetInt(tableDataKeysInfo[i]))
			}
			return &Collector{
				listenUdpPort:     jsonConfigInfos.GetInt("listenUdpPort"),
				listenIp:          jsonConfigInfos.GetString("listenIp"),
				kafkaInfo:         jsonConfigInfos.GetString("kafkaInfo"),
				kafkaTopic:        jsonConfigInfos.GetString("kafkaTopic"),
				jetLag:            jsonConfigInfos.GetInt("jetLag"),
				headLength:        jsonConfigInfos.GetInt("headLength"),
				messageLength:     jsonConfigInfos.GetInt("messageLength"),
				messageMap:        messageMap,
				tableDataMap:      tableDataMap,
				tableName:         jsonConfigInfos.GetString("tableName"),
				columnDataCollect: tableDataKeysInfo,
			}, nil
		}
	}
	return &Collector{
		listenUdpPort:     2055,
		listenIp:          "0.0.0.0",
		kafkaInfo:         "127.0.0.1:9092",
		kafkaTopic:        "netflow-log",
		jetLag:            0,
		headLength:        16,
		messageLength:     64,
		messageMap:        nil,
		tableDataMap:      nil,
		tableName:         "",
		columnDataCollect: nil,
	}, nil
}

/*
定义数据类型解析规则
*/
func parseRule(dataType string, start int, end int, buffer []byte) interface{} {
	switch dataType {
	case "DATE":
		if int64(binary.BigEndian.Uint32(buffer[start:end])) == 0 {
			return int64(0)
		} else {
			return int64(binary.BigEndian.Uint32(buffer[start:end])) * 1000
		}
	case "IP":
		return net.IP(buffer[start:end]).String()
	case "USHORT2":
		return int32(binary.BigEndian.Uint16(buffer[start:end]))
	case "ULONG4":
		return int64(binary.BigEndian.Uint32(buffer[start:end]))
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
	columnDataCollect := collector.columnDataCollect
	tableName := collector.tableName
	collectTableData := &TableData{}
	collectTableData.TableName = tableName
	for _, columnNameInfo := range columnDataCollect {
		collectTableData.Columns = append(collectTableData.Columns,
			&ColumnData{ColumnName: columnNameInfo, ColumnType: tableDataMap[columnNameInfo]})
	}
	return collectTableData
}

/*
固定格式生成table数据测试

			"out_second":6,
	       "source_ip":5,
	       "src_nat_ip":5,
	       "dest_ip":5,
	       "dest_nat_ip":5,
	       "src_port":1,
	       "src_nat_port":1,
	       "dest_port":1,
	       "dest_nat_port":1,
	       "start_time":6,
	       "end_time":6,
	       "in_total_pkg":2,
	       "in_total_byte":2,
	       "out_total_pkg":2,
	       "out_total_byte":2
*/
// func createTable(collector *Collector) *TableData {
// 	tableName := collector.tableName
// 	collectTableData := &TableData{}
// 	collectTableData.TableName = tableName
// 	collectTableData.Columns = append(collectTableData.Columns,
// 		&ColumnData{ColumnName: "out_second", ColumnType: 6},
// 		&ColumnData{ColumnName: "source_ip", ColumnType: 5},
// 		&ColumnData{ColumnName: "src_nat_ip", ColumnType: 5},
// 		&ColumnData{ColumnName: "dest_ip", ColumnType: 5},
// 		&ColumnData{ColumnName: "dest_nat_ip", ColumnType: 5},
// 		&ColumnData{ColumnName: "src_port", ColumnType: 1},
// 		&ColumnData{ColumnName: "src_nat_port", ColumnType: 1},
// 		&ColumnData{ColumnName: "dest_port", ColumnType: 1},
// 		&ColumnData{ColumnName: "dest_nat_port", ColumnType: 1},
// 		&ColumnData{ColumnName: "start_time", ColumnType: 6},
// 		&ColumnData{ColumnName: "end_time", ColumnType: 6},
// 		&ColumnData{ColumnName: "in_total_pkg", ColumnType: 2},
// 		&ColumnData{ColumnName: "in_total_byte", ColumnType: 2},
// 		&ColumnData{ColumnName: "out_total_pkg", ColumnType: 2},
// 		&ColumnData{ColumnName: "out_total_byte", ColumnType: 2})
// 	fmt.Println(collectTableData)
// 	return collectTableData
// }

/*
固定格式添加数据
*/
// func addTableData(collector *Collector, kafkaMessageKeyValues map[string]interface{}, tableData *TableData) {
// 	tableOneRow := &RowValue{}
// 	tableOneRow.FieldValue = append(tableOneRow.FieldValue,
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["out_second"].(int64)}},
// 		&FieldValue{Data: &FieldValue_S{S: kafkaMessageKeyValues["source_ip"].(string)}},
// 		&FieldValue{Data: &FieldValue_S{S: kafkaMessageKeyValues["src_nat_ip"].(string)}},
// 		&FieldValue{Data: &FieldValue_S{S: kafkaMessageKeyValues["dest_ip"].(string)}},
// 		&FieldValue{Data: &FieldValue_S{S: kafkaMessageKeyValues["dest_nat_ip"].(string)}},
// 		&FieldValue{Data: &FieldValue_I{I: kafkaMessageKeyValues["src_port"].(int32)}},
// 		&FieldValue{Data: &FieldValue_I{I: kafkaMessageKeyValues["src_nat_port"].(int32)}},
// 		&FieldValue{Data: &FieldValue_I{I: kafkaMessageKeyValues["dest_port"].(int32)}},
// 		&FieldValue{Data: &FieldValue_I{I: kafkaMessageKeyValues["dest_nat_port"].(int32)}},
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["start_time"].(int64)}},
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["end_time"].(int64)}},
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["in_total_pkg"].(int64)}},
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["in_total_byte"].(int64)}},
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["out_total_pkg"].(int64)}},
// 		&FieldValue{Data: &FieldValue_L{L: kafkaMessageKeyValues["out_total_byte"].(int64)}},
// 	)
// 	tableData.Rows = append(tableData.Rows, tableOneRow)
// 	fmt.Println(tableData)
// }

/*
字段数据添加
*/
func addTableData(collector *Collector, kafkaMessageKeyValues map[string]interface{}, tableData *TableData) {
	tableDataMap := collector.tableDataMap
	columnData := tableData.GetColumns()
	tableOneRow := &RowValue{}
	for _, columnDataInfo := range columnData {
		columnName := columnDataInfo.ColumnName
		tableDataInfo := kafkaMessageKeyValues[columnName]
		dataType := tableDataMap[columnName]
		switch dataType {
		case 1:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_I{I: tableDataInfo.(int32)}})
		case 2:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_L{L: tableDataInfo.(int64)}})
		case 3:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_D{D: tableDataInfo.(float64)}})
		case 4:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_B{B: tableDataInfo.(bool)}})
		case 5:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_S{S: tableDataInfo.(string)}})
		case 6:
			tableOneRow.FieldValue = append(tableOneRow.FieldValue,
				&FieldValue{Data: &FieldValue_L{L: tableDataInfo.(int64)}})
		}
	}
	tableData.Rows = append(tableData.Rows, tableOneRow)
	fmt.Println(tableData)
}
