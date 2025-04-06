package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// LocationData 表示从 VictoriaMetrics 查询返回的结构体
type LocationData struct {
	UserID    string  `json:"user_id"`
	W         float64 `json:"w"`
	J         float64 `json:"j"`
	TimeStamp int64   `json:"time_stamp"` // 毫秒
}

// 写入 VictoriaMetrics（使用 InfluxDB Line Protocol）
func writeToVictoriaMetrics(userID string, eventTS, collectTS int64, longitude, latitude float64) error {
	// Line Protocol 格式：
	// measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
	fmt.Println("传入参数:")
	fmt.Println("userID:", userID)
	fmt.Println("eventTS:", eventTS)
	fmt.Println("collectTS:", collectTS)
	fmt.Println("longitude:", longitude)
	fmt.Println("latitude:", latitude)

	line := fmt.Sprintf(
		"t_zbs,user_id=%s,u_ts=%d j=%f,w=%f\n",
		userID, eventTS, longitude, latitude,
	)

	fmt.Println("拼接的 Line Protocol 数据：", line) // 添加打印调试

	req, err := http.NewRequest("POST", "http://localhost:8428/api/v2/write", bytes.NewBuffer([]byte(line)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write failed: %s", string(bodyBytes))
	}

	return nil
}

// 从 VictoriaMetrics 查询数据
func queryFromVictoriaMetrics(userID string, startMs int64, endMs int64, metric string, stepSec int) ([]LocationData, error) {
	// 构造查询表达式
	query := fmt.Sprintf(`%s{user_id="%s"}`, metric, userID)

	// 创建 base URL
	baseURL := "http://localhost:8428/api/v1/query_range"

	// 创建 GET 请求对象
	req, err := http.NewRequest("GET", baseURL, nil)
	if err != nil {
		return nil, err
	}

	// 使用 url.Values 来模拟 --data-urlencode 参数
	params := url.Values{}
	params.Add("query", query)
	params.Add("start", fmt.Sprintf("%d", startMs/1000)) // 转换为秒
	params.Add("end", fmt.Sprintf("%d", endMs/1000))     // 转换为秒
	params.Add("step", fmt.Sprintf("%d", stepSec))

	// 将参数编码并设置为 RawQuery（这等价于 curl 的 --data-urlencode）
	req.URL.RawQuery = params.Encode()

	// 打印模拟出的完整 URL（带 urlencode）
	fmt.Println("最终请求 URL（模拟 --data-urlencode）:", req.URL.String())

	// 发起请求
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	fmt.Println("响应内容:", string(body))

	// 解析 JSON 结果
	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][]interface{}   `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	// 提取数据
	var data []LocationData
	for _, series := range result.Data.Result {
		userID := series.Metric["user_id"]
		for _, v := range series.Values {
			if len(v) != 2 {
				continue
			}
			timestampFloat, ok1 := v[0].(float64)
			valueStr, ok2 := v[1].(string)
			if !ok1 || !ok2 {
				continue
			}
			longitude := 0.0
			fmt.Sscanf(valueStr, "%f", &longitude)

			data = append(data, LocationData{
				UserID:    userID,
				J:         longitude, //经度
				W:         latitude,                          // 纬度 latitude
				TimeStamp: int64(timestampFloat * 1000), // 回到毫秒
			})
		}
	}

	return data, nil
}

// 示例程序入口
func main() {
	userID := "333"
	now := time.Now()
	collectTS := now.UnixMilli() // 当前时间戳，单位为毫秒 - ms
	eventTS := collectTS - 10000 // 举例，事件时间为采集前10秒
	longitude := 111.11
	latitude := 111.11

	fmt.Println("🧪 当前版本: 测试111.11版本")
	//写入数据
	err := writeToVictoriaMetrics(userID, eventTS, collectTS, longitude, latitude)
	if err != nil {
		fmt.Println("❌ 写入失败:", err)
		return
	}
	fmt.Println("✅ 数据写入成功") //success

	// 设置 userID 和当前时间戳
	endMs := now.UnixMilli()      // 当前时间戳，单位为毫秒
	startMs := endMs - 10800*1000 // 1 小时前
	step := 8640000               //100天(86400/天)

	// 调用查询函数
	// data, err := queryFromVictoriaMetrics(userID, startMs, endMs, "t_zbs_j", 10)
	data, err := queryFromVictoriaMetrics(userID, startMs, endMs, "j", step)
	if err != nil {
		log.Fatalf("查询失败: %v\n", err) //failed
	}

	// 输出查询到的数据
	if len(data) == 0 {
		fmt.Println("没有查询到数据。") //No data
	}

}
