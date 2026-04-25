package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/go-sql-driver/mysql"
)

type InputUser struct {
	Nom string `json:"nom"`
}

type OutputUser struct {
	Nom   string `json:"nom"`
	Email string `json:"email,omitempty"`
}

type PendingMessage struct {
	Key  []byte
	User InputUser
}

var totalProduced atomic.Int64
var endOnce sync.Once

const (
	inputTopic      = "users"
	outputTopic     = "notification"
	workerCount     = 4
	batchSize       = 100
	flushInterval   = 250 * time.Millisecond
	jobsBufferSize  = 5000
	dbMaxOpenConns  = 20
	dbMaxIdleConns  = 10
)

func main() {
	log.Println("🚀 H2 Optimized ConcurrentDB + Batch SQL + Batch Kafka")

	kafkaServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if kafkaServers == "" {
		kafkaServers = "kafka-0.kafka:9092"
	}

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "root:pass@tcp(mysql:3306)/uqar"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("DB:", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(dbMaxOpenConns)
	db.SetMaxIdleConns(dbMaxIdleConns)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatal("DB ping:", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServers,
		"linger.ms":         100,
		"batch.num.messages": 10000,
	})
	if err != nil {
		log.Fatal("Producer:", err)
	}
	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaServers,
		"group.id":          "user-processor-h2-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("Consumer:", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{inputTopic}, nil); err != nil {
		log.Fatal("Subscribe:", err)
	}

	jobs := make(chan kafka.Message, jobsBufferSize)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerBatch(id, db, producer, jobs)
		}(i)
	}

	log.Printf("%d workers démarrés | batchSize=%d | flushInterval=%v", workerCount, batchSize, flushInterval)

	for {
		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				log.Printf("Consumer message error: %v", e.TopicPartition.Error)
				continue
			}
			jobs <- *e

		case kafka.Error:
			log.Printf("Kafka error: %v", e)

		default:
		}
	}
}

func workerBatch(id int, db *sql.DB, producer *kafka.Producer, jobs <-chan kafka.Message) {
	log.Printf("🚀 Worker %d démarré", id)

	pending := make([]PendingMessage, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func(reason string) {
		if len(pending) == 0 {
			return
		}

		start := time.Now()
		outMessages, err := enrichBatch(db, pending)
		if err != nil {
			log.Printf("Worker %d enrichBatch error: %v", id, err)
			pending = pending[:0]
			return
		}

		sendBatch(id, producer, outMessages)

		log.Printf(
			"Worker %d flush reason=%s | in=%d | out=%d | total=%d | took=%v",
			id,
			reason,
			len(pending),
			len(outMessages),
			totalProduced.Load(),
			time.Since(start),
		)

		pending = pending[:0]
	}

	for {
		select {
		case msg, ok := <-jobs:
			if !ok {
				flush("channel-closed")
				log.Printf("Worker %d terminé", id)
				return
			}

			raw := string(msg.Value)

			if raw == "%%END%%" {
				flush("end-marker")

				endOnce.Do(func() {
					log.Printf("Worker %d forward %%END%% to %s", id, outputTopic)
					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     stringPtr(outputTopic),
							Partition: kafka.PartitionAny,
						},
						Key:   msg.Key,
						Value: msg.Value,
					}, nil)
					if err != nil {
						log.Printf("Worker %d produce END error: %v", id, err)
					}
				})

				continue
			}

			var input InputUser
			if err := json.Unmarshal(msg.Value, &input); err != nil {
				log.Printf("Worker %d JSON err: %v", id, err)
				continue
			}

			pending = append(pending, PendingMessage{
				Key:  msg.Key,
				User: input,
			})

			if len(pending) >= batchSize {
				flush("batch-full")
			}

		case <-ticker.C:
			flush("ticker")
		}
	}
}

func enrichBatch(db *sql.DB, pending []PendingMessage) ([]kafka.Message, error) {
	if len(pending) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(pending))
	seen := make(map[string]struct{}, len(pending))

	for _, p := range pending {
		if _, ok := seen[p.User.Nom]; ok {
			continue
		}
		seen[p.User.Nom] = struct{}{}
		names = append(names, p.User.Nom)
	}

	placeholders := make([]string, len(names))
	args := make([]any, len(names))
	for i, name := range names {
		placeholders[i] = "?"
		args[i] = name
	}

	query := fmt.Sprintf(
		"SELECT nom, email FROM etudiants WHERE nom IN (%s)",
		strings.Join(placeholders, ","),
	)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	emailByName := make(map[string]string, len(names))
	for rows.Next() {
		var nom, email string
		if err := rows.Scan(&nom, &email); err != nil {
			return nil, err
		}
		emailByName[nom] = email
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]kafka.Message, 0, len(pending))
	for _, p := range pending {
		output := OutputUser{
			Nom:   p.User.Nom,
			Email: emailByName[p.User.Nom],
		}

		enriched, err := json.Marshal(output)
		if err != nil {
			return nil, err
		}

		out = append(out, kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     stringPtr(outputTopic),
				Partition: kafka.PartitionAny,
			},
			Key:   p.Key,
			Value: enriched,
		})
	}

	return out, nil
}

func sendBatch(id int, producer *kafka.Producer, batch []kafka.Message) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	for i := range batch {
		if err := producer.Produce(&batch[i], nil); err != nil {
			log.Printf("Worker %d produce error: %v", id, err)
		}
	}

	total := totalProduced.Add(int64(len(batch)))
	log.Printf("Worker %d 📤 Batch %d msgs en %v (total envoyé: %d)", id, len(batch), time.Since(start), total)
}

func stringPtr(s string) *string {
	return &s
}