package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/labstack/echo"
)

func main() {

	fmt.Println("Start listening")

	route := newRoute()
	route.GET("/health", handlerHelpCheck)

	go func() {
		go run(route)
	}()

	// kafka
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "10.140.206.74",
		"group.id":          "hello-kafka",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Println("connect kafka fail")
		log.Fatal(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	c.SubscribeTopics([]string{"hello-kafka", "^aRegex.*[Tt]opic"}, nil)
	go func() {
		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}()
	fmt.Println("awaiting signal")

	<-quit
	c.Close()
	fmt.Println("exiting")
}

func newRoute() *echo.Echo {
	e := echo.New()

	e.HideBanner = true
	// e.HidePort = true
	return e
}

func run(e *echo.Echo) {
	e.Logger.Fatal(e.Start(":1234"))
}

func handlerHelpCheck(c echo.Context) error {
	fmt.Println("health checker")
	return c.String(http.StatusOK, "hello!")
}
