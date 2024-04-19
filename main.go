package main

import (
	"fmt"
	"net"
	"os"

	kafka "github.com/chaolihf/udpgo/kafka"
	"google.golang.org/protobuf/proto"
)

func main() {
	collector, _ := newNetCollector()
	kafkaTopic := collector.kafkaTopic
	// 监听2055端口的UDP协议
	addr := &net.UDPAddr{
		Port: collector.listenUdpPort,
		IP:   net.ParseIP(collector.listenIp), // 监听所有网络接口
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		logger.Log("Error listening on port %d: %s\n", collector.listenUdpPort, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Listening on UDP port %d...\n", collector.listenUdpPort)

	p := kafka.InitKafka([]string{collector.kafkaInfo})
	//循环接收数据
	for {
		tableData := createTable(collector)
		// 创建一个足够大的缓冲区来接收数据
		buffer := make([]byte, 1024)
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		buffer = buffer[:n]
		if err != nil {
			logger.Log("Error reading from UDP: %s\n", err)
			continue
		}
		fmt.Printf("Received %d bytes from %s: %x\n", n, remoteAddr, buffer[:n])
		messageMap := collector.messageMap
		jetLag := collector.jetLag
		if messageMap == nil {
			fmt.Printf("解析规则未配置")
		} else {
			kafkaHeadKeyValues := make(map[string]interface{})
			for key, value := range messageMap {
				start, end, dataType, location := stringSplit(value)
				if location == 0 {
					kafkaHeadKeyValues[key] = parseRule(dataType, start, end, buffer, jetLag)
				}
			}
			buffer = buffer[collector.headLength:]
			for i := 0; i < len(buffer); i += collector.messageLength {
				kafkaMessageKeyValues := make(map[string]interface{})
				block := buffer[i : i+collector.messageLength]
				for key, value := range messageMap {
					start, end, dataType, location := stringSplit(value)
					if location == 1 {
						kafkaMessageKeyValues[key] = parseRule(dataType, start, end, block, jetLag)
					}
				}
				for key, value := range kafkaHeadKeyValues {
					kafkaMessageKeyValues[key] = value
				}
				addTableData(collector, kafkaMessageKeyValues, tableData)
				protoBytes, err := proto.Marshal(tableData)
				if err != nil {
					logger.Log("Error marshaling PROTO:", err)
				}
				p.SendMesssage(protoBytes, kafkaTopic, "netflow-log")
			}
		}
	}
}
