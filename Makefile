produce:
	go run ./cmd/producer/main.go

consume:
	go run ./cmd/consumer/main.go

up:
	docker compose down --remove-orphans
	docker compose up

cli-consumer:
	docker exec -it kaf kafka-console-consumer --bootstrap-server localhost:9092 --topic new-topic
