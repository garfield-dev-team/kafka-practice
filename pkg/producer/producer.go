package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	address := []string{"127.0.0.1:9092"}
	//配置发布者
	config := sarama.NewConfig()
	/**
	sarama.WaitForAll
		表示生产者会等待所有的副本都成功收到消息并发送确认消息给生产者
		然后才会认为消息发送成功。这是最安全的确认机制，可以确保消息不会丢失，但会增加一定的延迟和网络开销
	sarama.WaitForLocal
		默认值。表示只需要等待本地副本收到消息并确认。本地副本指的是生产者所在的 Kafka Broker 上的领导者副本
		其他追随者副本可能会稍后复制该消息，但生产者不会等待它们的确认
	 	好处是可以减少等待确认的时间和网络开销，适用于对消息实时性要求较高、可靠性要求较低的场景
	sarama.NoResponse
		表示生产者在发送消息后不需要等待任何确认
	*/
	//config.Producer.RequiredAcks = sarama.WaitForAll

	/**
	sarama.NewHashPartitioner
		默认值。哈希分区器，根据消息的 key 进行哈希计算，然后将消息分配到对应的分区。
		相同 key 的消息将始终被分配到同一个分区，可以保证具有相同 key 的消息按顺序被处理
	sarama.NewRandomPartitioner
		随机分区器，随机选择一个分区来存储消息。适用于消息的顺序无关紧要的场景
	sarama.NewRoundRobinPartitioner
		轮询分区器，按照轮询的方式依次选择分区来存储消息。适用于分区间负载均衡的场景
	*/
	//config.Producer.Partitioner = sarama.NewRandomPartitioner

	//确认返回，记得一定要写，因为本次例子用的是同步发布者
	config.Producer.Return.Successes = true
	//设置超时时间 这个超时时间一旦过期，新的订阅者在这个超时时间后才创建的，就不能订阅到消息了
	config.Producer.Timeout = 5 * time.Second
	//连接发布者，并创建发布者实例
	p, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		log.Printf("sarama.NewSyncProducer err, message=%s \n", err)
		return
	}
	//程序退出时释放资源
	defer p.Close()
	//设置一个逻辑上的分区名
	topic := "anyanfei"
	//这个是发布的内容
	srcValue := "sync: this is a message. index=%d"
	//发布者循环发送0-9的消息内容
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf(srcValue, i)
		//创建发布者消息体
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}
		//发送消息并返回消息所在的物理分区和偏移量
		partition, offset, err := p.SendMessage(msg)
		if err != nil {
			log.Printf("send message(%s) err=%s \n", value, err)
		} else {
			_, _ = fmt.Fprintf(os.Stdout, value+"发送成功，partition=%d, offset=%d \n", partition, offset)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
