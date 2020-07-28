package main

import (
	pb "cache-consumer/message_bus"
	"context"
	"flag"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
)

var (
	address = flag.String("host", "127.0.0.1:10000", "Host address")
)

func main() {
	conn, err := grpc.Dial(*address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMessageBusClient(conn)

	populateJobs(c)
	time.Sleep(time.Second * 1e3)
}

func populateJobs(c pb.MessageBusClient) {
	for i := 0; i < 1e3; i++ {
		go requestJob(c)
	}
}

func requestJob(c pb.MessageBusClient) {
	for {
		request(c)
	}
}

func request(c pb.MessageBusClient) {
	stream, err := c.GetRandomDataStream(context.Background(), &pb.Request{})
	if err != nil {
		log.Printf("grpc error: %v\n", err)
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("stream error: %v\n", err)
			break
		}
		log.Println(resp.Message)
	}
}
