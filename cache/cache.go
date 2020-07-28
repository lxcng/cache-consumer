package main

import (
	pb "cache-consumer/message_bus"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

var (
	port = flag.Int("port", 10000, "The server port")
)

var (
	conf = &config{}
	rdb  *redis.Client
)

type config struct {
	URLs             []string `yaml:"URLs"`
	MinTimeout       int      `yaml:"MinTimeout"`
	MaxTimeout       int      `yaml:"MaxTimeout"`
	NumberOfRequests int      `yaml:"NumberOfRequests"`
}

func init() {
	bt, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	err = yaml.Unmarshal(bt, conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err = rdb.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	fmt.Println(*conf)

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	log.Printf("cache listening %s\n", lis.Addr().String())
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterMessageBusServer(grpcServer, newServer())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}

func newServer() *routeGuideServer {
	s := &routeGuideServer{}
	return s
}

type routeGuideServer struct {
	pb.UnimplementedMessageBusServer
}

func (*routeGuideServer) GetRandomDataStream(req *pb.Request, stream pb.MessageBus_GetRandomDataStreamServer) error {
	results := traverseURLS(stream.Context())
	for res := range results {
		err := stream.Send(&pb.Reply{Message: res})
		if err != nil {
			return err
		}
	}
	return nil
}

func traverseURLS(ctx context.Context) chan string {
	results := make(chan string)
	wg := sync.WaitGroup{}
	wg.Add(conf.NumberOfRequests)
	go func() {
		wg.Wait()
		close(results)
	}()
	for i := 0; i < conf.NumberOfRequests; i++ {
		go getCachedURL(ctx, getRandURL(), results, &wg)
	}
	return results
}

func getRandURL() string {
	i := rand.Intn(len(conf.URLs))
	return conf.URLs[i]
}

func getRandEx() time.Duration {
	i := rand.Intn(conf.MaxTimeout - conf.MinTimeout + 1)
	return time.Duration(conf.MinTimeout+i) * time.Millisecond
	// return 15 * time.Second
}

// getCachedURL returns cached responce from redis or stores response with double commit
func getCachedURL(ctx context.Context, url string, results chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	routineId := uuid.New()
	// try to put lock on url
	rdb.SetNX(url, routineId.String(), 0)

	log.Printf("getting %s, with lock %s\n", url, routineId.String())
	result := ""
	for i := 0; i < 20; i++ {
		// get url cached responce/lock id
		res, err := rdb.Get(url).Result()
		if err != nil {
			continue
		}
		if lock, err := uuid.Parse(res); err == nil {
			if lock.String() == routineId.String() {
				log.Printf("got lock on %s\n", url)
				// query url if lock id in redis equals to goroutine id
				res, err := getURL(ctx, url)
				if err != nil {
					log.Printf("got cached error for %s\n", url)
					rdb.Set(url, err.Error(), getRandEx())
					result = err.Error()
				} else {
					log.Println(err)
					rdb.Set(url, res, getRandEx())
					result = res
				}
				break
			}
			// wait 50 ms
			log.Printf("waiting for %s\n", url)
			time.Sleep(time.Millisecond * 50)
		} else {
			// got non uuid data from redis, hence cached responce
			log.Printf("got cached resp for %s\n", url)
			result = res
			break
		}
	}

	results <- result
}

func getURL(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	bt, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(bt), nil
}
