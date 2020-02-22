package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"offline_parser/utils"
	"time"

	"github.com/streadway/amqp"
)

// path to dir inside service binded to -> prod zips directory
const dirtyZipPath = "/zips/"


// for debug purposes, remove
func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {

	// wait untill rabbit is up
	time.Sleep(20 * time.Second)
	log.Println("PRODUCER CALLING RABBIT")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:5672/", utils.RabbitServiceName))
	handleError(err, "Can't connect to AMQP")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	handleError(err, "Can't create a amqpChannel")

	defer amqpChannel.Close()

	queue, err := amqpChannel.QueueDeclare(utils.ZipNamesQueue, true, false, false, false, nil)
	handleError(err, "Could not declare `add` queue")

	// read contents of directory, volume to dirtyZips
	files, err := ioutil.ReadDir(dirtyZipPath)
	if err != nil {
		utils.HandleError(err, fmt.Sprintf("Err reading dir %s check permission granted", dirtyZipPath))
		panic(err)
	}


	zipNames := make([]string, 0, 0)
	for _, f := range files {
		if !f.IsDir() {
			zipNames = append(zipNames, f.Name())
		}
	}

	log.Printf("Producing %d zipnames...", len(zipNames))
	stopChan := make(chan bool)
	for _, name := range zipNames {
		// do need encode/decode?
		err = amqpChannel.Publish("", queue.Name, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(name),
		})

		if err != nil {
			log.Fatalf("Error publishing message: %s", err)
		}
		log.Printf("Producing task with name %s", name)
	}
	log.Println("DONE PRODUCING!")
	// blocks
	<-stopChan
}