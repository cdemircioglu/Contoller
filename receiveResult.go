package main

import (
	"log"
	"fmt"
	"net"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}


var chanNextLine chan string = make(chan string)
var chanRegister chan chan string = make(chan chan string)

func main() {
	
	// listen on all interfaces
	ln, _ := net.Listen("tcp", "localhost:8081")

	
	
	for {	
		// accept connection on port
		conn, _ := ln.Accept()

		var xmlmsg string 
		xmlmsg = getMessage()
		fmt.Println(xmlmsg)	
		conn.Write([]byte(xmlmsg + "\n"))		
		
		go handleConn(conn)
	}
	
	
}

func doWrites(conn net.Conn, ch chan string) {
	for {
		str, ok := <-ch
		if !ok {
			return
		}
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, err := conn.Write([]byte(str))
		if err != nil {
			conn.Close()
			return
		}
	}
}


func handleConn(conn net.Conn) {
	log.Println("Connection opened from", conn.RemoteAddr())
	ch := make(chan string, 1000)
	chanRegister <- ch
	go doWrites(conn, ch)

	buf := make([]byte, 1, 1)
	conn.Read(buf)
	chanUnregister <- ch
	conn.Close()
	close(ch)
	log.Println("Connection closed from ", conn.RemoteAddr())
}


func getMessage() string {
	var msg string	
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"workresult", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)	
	
	go func() {
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)
			msg = string(d.Body[:])			
			forever <- true
		}

	}()

	//log.Printf(" [*] Waiting for messages. To exit press CTRL+C -dc")
	<-forever
	return msg
}