package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/go-redis/redis"
)

var subject string = "tickets"
var consumersGroup string = "tickets-consumer-group"
var wg = &sync.WaitGroup{}

func main() {
	log.Println("Consumer started")

	client := getRedis()
	createConsumerGroup(client)
	processTickets(client)

	// redisClient := redis.NewClient(&redis.Options{
	// 	Addr: fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
	// })
	// _, err := redisClient.Ping().Result()
	// if err != nil {
	// 	log.Fatal("Unable to connect to Redis", err)
	// }

	// log.Println("Connected to Redis server")

	// redisClient := getRedis()

	// subject := "tickets"
	// consumersGroup := "tickets-consumer-group"

	// err = redisClient.XGroupCreate(subject, consumersGroup, "0").Err()
	// if err != nil {
	// 	log.Println(err)
	// }

	// uniqueID := xid.New().String()

	// for {
	// 	entries, err := redisClient.XReadGroup(&redis.XReadGroupArgs{
	// 		Group:    consumersGroup,
	// 		Consumer: uniqueID,
	// 		Streams:  []string{subject, ">"},
	// 		Count:    2,
	// 		Block:    0,
	// 		NoAck:    false,
	// 	}).Result()
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	for i := 0; i < len(entries[0].Messages); i++ {
	// 		messageID := entries[0].Messages[i].ID
	// 		values := entries[0].Messages[i].Values
	// 		eventDescription := fmt.Sprintf("%v", values["whatHappened"])
	// 		ticketID := fmt.Sprintf("%v", values["ticketID"])
	// 		ticketData := fmt.Sprintf("%v", values["ticketData"])

	// 		if eventDescription == "ticket received" {
	// 			err := handleNewTicket(ticketID, ticketData)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}
	// 			redisClient.XAck(subject, consumersGroup, messageID)
	// 		}
	// 	}
	// }
}

func handleNewTicket(ticketID string, ticketData string) error {
	log.Printf("Handling new ticket id : %s data %s\n", ticketID, ticketData)
	// time.Sleep(100 * time.Millisecond)
	return nil
}

func getEnv(envName, valueDefault string) string {
	value := os.Getenv(envName)
	if value == "" {
		return valueDefault
	}
	return value
}

func getRedis() *redis.Client {
	// Create Redis Client
	var (
		host     = getEnv("REDIS_HOST", "localhost")
		port     = string(getEnv("REDIS_PORT", "6379"))
		password = getEnv("REDIS_PASSWORD", "")
	)

	client := redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: password,
		DB:       0,
	})

	// Check we have correctly connected to our Redis server
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal("Unable to connect to Redis", err)
	}

	log.Println("Connected to Redis server")

	return client
}

func createConsumerGroup(client *redis.Client) {
	err := client.XGroupCreate(subject, consumersGroup, "0").Err()
	if err != nil {
		log.Println("Unable to create consumergroup", err)
	}
}

func processTickets(client *redis.Client) {
	for {
		entries, err := client.XReadGroup(&redis.XReadGroupArgs{
			Group:    consumersGroup,
			Consumer: "test",
			Streams:  []string{subject, ">"},
			Count:    2,
			Block:    0,
			NoAck:    false,
		}).Result()
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(entries[0].Messages); i++ {
			messageID := entries[0].Messages[i].ID
			values := entries[0].Messages[i].Values
			eventDescription := fmt.Sprintf("%v", values["whatHappened"])
			ticketID := fmt.Sprintf("%v", values["ticketID"])
			ticketData := fmt.Sprintf("%v", values["ticketData"])

			if eventDescription == "ticket received0" {
				handleNewTicket(ticketID, ticketData)

				// if err != nil {
				// 	log.Fatal(err)
				// }
				client.XAck(subject, consumersGroup, messageID)
			}
		}
	}
}
