Run redis:

`docker run -p 6379:6379 -d redis`

Run cache:

`go run cache/cache.go`

Run consumer:

`go run consumer/consumer.go`