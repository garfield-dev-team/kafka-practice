package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Consumer .
// 重写订阅者，并重写订阅者的所有方法
type Consumer struct {
	ready chan bool
}

// Setup .
// Setup方法在新会话开始时运行的，然后才使用声明
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup .
// 一旦所有的订阅者协程都退出，Cleanup方法将在会话结束时运行
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim .
// 订阅者在会话中消费消息，并标记当前消息已经被消费。
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		// 将消息标记为已消费，实际上就是提交偏移量
		// 也可以配置 `config.Consumer.Offsets.AutoCommit.Enable = true` 自动提交
		session.MarkMessage(message, "")
	}

	return nil
}

func newKafkaConsumer() {
	address := []string{"127.0.0.1:9092"}
	/**
	group:
	  设置订阅者群 如果多个订阅者group一样，则随机挑一个进行消费，当然也可以设置轮训，在设置里面修改；
	  若多个订阅者的group不同，则一旦发布者发布消息，所有订阅者都会订阅到同样的消息；
	topics:
	  逻辑分区必须与发布者相同，还是用安彦飞，不然找不到内容咯
	  当然订阅者是可以订阅多个逻辑分区的，只不过因为演示方便我写了一个，你可以用英文逗号分割在这里写多个
	*/
	var (
		group  = "Consumer1"
		topics = "anyanfei"
	)
	log.Println("Starting a new Sarama consumer")
	//配置订阅者
	config := sarama.NewConfig()
	//配置偏移量
	//当设置为 sarama.OffsetNewest 时，消费者会从当前可用的最新消息开始消费，跳过之前已经被消费者处理过的消息
	//当设置为 sarama.OffsetOldest 时，消费者会从最早可用的消息开始消费，包括之前已经被消费者处理过的消息
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//如果你希望标记已消费的消息，你可以在消费消息后手动提交消费的偏移量
	//sarama 将会从上次提交的偏移量继续消费消息，而不会重复消费已经处理过的消息
	//这边注意，Kafka 不会自动删除已消费的消息，而是根据配置的保留策略来决定消息的保留时间或保留大小
	config.Consumer.Offsets.AutoCommit.Enable = true
	//开始创建订阅者
	consumer := &Consumer{
		ready: make(chan bool),
	}
	//创建一个上下文对象，实际项目中也一定不要设置超时（当然，按你项目需求，我是没见过有项目需求要多少时间后取消订阅的）
	ctx, cancel := context.WithCancel(context.Background())
	//创建订阅者群，集群地址发布者代码里已定义
	client, err := sarama.NewConsumerGroup(address, group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	//创建同步组
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			/**
			  官方说：`订阅者`应该在无限循环内调用
			  当`发布者`发生变化时
			  需要重新创建`订阅者`会话以获得新的声明

			  所以这里把订阅者放在了循环体内
			*/
			if err := client.Consume(ctx, strings.Split(topics, ","), consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// 检查上下文是否被取消，收到取消信号应当立刻在本协程中取消循环
			if ctx.Err() != nil {
				return
			}
			//获取订阅者准备就绪信号
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // 获取到了订阅者准备就绪信号后打印下面的话
	log.Println("Sarama consumer up and running!...")

	//golang优雅退出的信号通道创建
	sigterm := make(chan os.Signal, 1)
	//golang优雅退出的信号获取
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	//创建选择器，如果不是上下文取消或者用户ctrl+c这种系统级退出，则就不向下执行了
	//注意，启动消费者之后，主 goroutine 会阻塞在下面 select 语句
	//如果监听到上下文取消，或者系统信号，会调用 cancel 方法并等待子 goroutine 退出，最后关闭客户端
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}

	cancel()
	wg.Wait()

	//关闭客户端
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func main() {
	newKafkaConsumer()
}
