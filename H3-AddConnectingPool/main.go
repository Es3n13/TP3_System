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

const (
	inputTopic        = "users"
	outputTopic       = "notification"
	consumerGroupID   = "user-processor-h3-group"
	aggregatorCount   = 4
	dbWorkerCount     = 4
	batchSize         = 120
	flushInterval     = 250 * time.Millisecond
	jobsBufferSize    = 5000
	dbTasksBufferSize = 5000
	dbMaxOpenConns    = 20
	dbMaxIdleConns    = 10
)

func main() {
	log.Println("🚀 H3 ConcurrentDB + Connection Pool + Batch SQL + Batch Kafka")

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
	db.SetConnMaxIdleTime(2 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatal("DB ping:", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaServers,
		"linger.ms":          100,
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
		"group.id":          consumerGroupID,
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
	dbTasks := make(chan []PendingMessage, dbTasksBufferSize)

	var aggWG sync.WaitGroup
	for i := 0; i < aggregatorCount; i++ {
		aggWG.Add(1)
		go func(id int) {
			defer aggWG.Done()
			aggregator(id, jobs, dbTasks)
		}(i)
	}

	var dbWG sync.WaitGroup
	for i := 0; i < dbWorkerCount; i++ {
		dbWG.Add(1)
		go func(id int) {
			defer dbWG.Done()
			dbWorker(id, db, producer, dbTasks)
		}(i)
	}

	log.Printf(
		"%d aggregators + %d DB workers démarrés | batchSize=%d | flushInterval=%v",
		aggregatorCount, dbWorkerCount, batchSize, flushInterval,
	)

	var endKey []byte

consumeLoop:
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

			if string(e.Value) == "%%END%%" {
				log.Printf("Consumer received %%END%%, stopping intake")
				endKey = e.Key
				break consumeLoop
			}

			jobs <- *e

		case kafka.Error:
			log.Printf("Kafka error: %v", e)

		default:
		}
	}

	close(jobs)
	aggWG.Wait()
	close(dbTasks)
	dbWG.Wait()

	log.Printf("All aggregators and DB workers completed, forwarding single %%END%% to %s", outputTopic)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     stringPtr(outputTopic),
			Partition: kafka.PartitionAny,
		},
		Key:   endKey,
		Value: []byte("%%END%%"),
	}, nil); err != nil {
		log.Printf("Produce END error: %v", err)
	}

	producer.Flush(5000)

	log.Printf("✅ H3 terminé | total envoyé: %d", totalProduced.Load())
}

func aggregator(id int, jobs <-chan kafka.Message, dbTasks chan<- []PendingMessage) {
	log.Printf("🚀 Aggregator %d démarré", id)

	pending := make([]PendingMessage, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func(reason string) {
		if len(pending) == 0 {
			return
		}

		batchCopy := make([]PendingMessage, len(pending))
		copy(batchCopy, pending)
		dbTasks <- batchCopy

		log.Printf("Aggregator %d flush reason=%s | in=%d", id, reason, len(pending))
		pending = pending[:0]
	}

	for {
		select {
		case msg, ok := <-jobs:
			if !ok {
				flush("channel-closed")
				log.Printf("Aggregator %d terminé", id)
				return
			}

			var input InputUser
			if err := json.Unmarshal(msg.Value, &input); err != nil {
				log.Printf("Aggregator %d JSON err: %v", id, err)
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

func dbWorker(id int, db *sql.DB, producer *kafka.Producer, dbTasks <-chan []PendingMessage) {
	log.Printf("🗄️ DB Worker %d démarré", id)

	for batch := range dbTasks {
		start := time.Now()

		outMessages, err := enrichBatch(batch)
		if err != nil {
			log.Printf("DB Worker %d enrichBatch prep error: %v", id, err)
			continue
		}

		if err := fillEmailsFromDB(db, outMessages); err != nil {
			log.Printf("DB Worker %d DB fill error: %v", id, err)
			continue
		}

		sendBatch(id, producer, outMessages)

		log.Printf(
			"DB Worker %d flush reason=batch-ready | in=%d | out=%d | took=%v",
			id, len(batch), len(outMessages), time.Since(start),
		)
	}

	log.Printf("DB Worker %d terminé", id)
}

func enrichBatch(pending []PendingMessage) ([]kafka.Message, error) {
	out := make([]kafka.Message, 0, len(pending))

	for _, p := range pending {
		output := OutputUser{
			Nom: p.User.Nom,
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

func fillEmailsFromDB(db *sql.DB, outMessages []kafka.Message) error {
	if len(outMessages) == 0 {
		return nil
	}

	names := make([]string, 0, len(outMessages))
	seen := make(map[string]struct{}, len(outMessages))

	for _, msg := range outMessages {
		var out OutputUser
		if err := json.Unmarshal(msg.Value, &out); err != nil {
			return err
		}
		if _, ok := seen[out.Nom]; ok {
			continue
		}
		seen[out.Nom] = struct{}{}
		names = append(names, out.Nom)
	}

	if len(names) == 0 {
		return nil
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
		return err
	}
	defer rows.Close()

	emailByName := make(map[string]string, len(names))
	for rows.Next() {
		var nom, email string
		if err := rows.Scan(&nom, &email); err != nil {
			return err
		}
		emailByName[nom] = email
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for i := range outMessages {
		var out OutputUser
		if err := json.Unmarshal(outMessages[i].Value, &out); err != nil {
			return err
		}

		out.Email = emailByName[out.Nom]

		enriched, err := json.Marshal(out)
		if err != nil {
			return err
		}
		outMessages[i].Value = enriched
	}

	return nil
}

func sendBatch(id int, producer *kafka.Producer, batch []kafka.Message) {
	if len(batch) == 0 {
		return
	}

	start := time.Now()

	for i := range batch {
		if err := producer.Produce(&batch[i], nil); err != nil {
			log.Printf("DB Worker %d produce error: %v", id, err)
		}
	}

	total := totalProduced.Add(int64(len(batch)))
	log.Printf("DB Worker %d 📤 Batch %d msgs en %v (total envoyé: %d)", id, len(batch), time.Since(start), total)
}

func stringPtr(s string) *string {
	return &s
}