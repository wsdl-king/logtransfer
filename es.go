package main

import (
	"encoding/json"
	"github.com/astaxie/beego/logs"
	"gopkg.in/olivere/elastic.v2"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var waitGroup sync.WaitGroup

var client *elastic.Client

const base_format = "2006-01-02 15:04:05"
const md = "(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8])))"
const hsm = "([0-1][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])"
const blank = "\\s"

func initEs(addr string) (err error) {
	// 使用客户端去嗅探整个集群的状态,多个节点的话, 我挂一个节点完全没问题
	client, err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(addr))
	if err != nil {
		logs.Error("connect to es error:%v", err)
		return
	}
	logs.Debug("conn to es success")
	return
}

func reloadKafka(topicArray []string) {
	for _, topic := range topicArray {
		kafkaMgr.AddTopic(topic)
	}
}

// 新的配置监听改变
func reload() {
	//GetLogConf() 从channel中获topic信息，而这部分信息是从etcd放进去的
	for conf := range GetLogConf() {
		var topicArray []string
		err := json.Unmarshal([]byte(conf), &topicArray)
		if err != nil {
			logs.Error("unmarshal failed,err:%v conf:%v", err, conf)
			continue
		}
		reloadKafka(topicArray)
	}
}

func Run(esThreadNum int) (err error) {
	go reload()
	for i := 0; i < esThreadNum; i++ {
		waitGroup.Add(1)
		go sendToEs()
	}
	waitGroup.Wait()
	return
}

type EsMessage struct {
	Message string
	Unix    int64
}

func sendToEs() {
	// 从msgChan中读取日志内容并扔到elasticsearch中
	for msg := range GetMessage() {
		var esMsg EsMessage
		esMsg.Message = msg.line
		fromString := getDateFromString(msg.line)
		esMsg.Unix = fromString
		_, err := client.Index().Index(msg.topic).Type(msg.topic).BodyJson(esMsg).Do()
		if err != nil {
			logs.Error("send to es failed,err:%v", err)
			continue
		}
		logs.Debug("send to es success")
	}
	waitGroup.Done()
}

func getDateFromString(s string) int64 {
	var unix int64 = 0
	regx, e := regexp.Compile(md + blank + hsm) // 创建正则表达式规则对象
	str1 := regx.FindAllString(s, 1)            // -1 代表找到所有的
	if str1 != nil && e == nil {
		//匹配到正确的时间
		date := strconv.Itoa(time.Now().Year()) + "-" + str1[0]
		t, e := time.Parse(base_format, date)
		if e == nil {
			unix = t.Unix()
		}
	}
	return unix
}
