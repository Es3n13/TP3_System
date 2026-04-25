package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/go-sql-driver/mysql"
)

const (
	numWorkers = 10
	bufferSize = 200
	inputTopic = "users"
)

type UserMsg struct {
	Nom    string `json:"nom"`
	Prenom string `json:"prenom"`
}

type EnrichedMsg struct {
	Nom    string `json:"nom"`
	Prenom string `json:"prenom"`
	Email  string `json:"email"`
}

var (
	db       *sql.DB
	msgChan  chan *kafka.Message
	wg       sync.WaitGroup
	outTopic = "notification"
)

func initDB() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		log.Fatal("MYSQL_DSN manquant")
	}

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("mysql open: %v", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("mysql ping: %v", err)
	}

	log.Println("✅ MySQL pool prêt")
}

func lookupEmail(fullName string) (string, error) {
	var email string

	query := `
		SELECT email
		FROM etudiants
		WHERE LOWER(nom) = LOWER(?)
		LIMIT 1
	`

	err := db.QueryRow(query, fullName).Scan(&email)
	if err != nil {
		return "", err
	}

	return email, nil
}

func forwardRawMessage(producer *kafka.Producer, value []byte) {
	topic := outTopic
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}, nil)

	if err != nil {
		log.Printf("❌ kafka raw forward error: %v", err)
	}
}

func worker(id int, producer *kafka.Producer) {
	defer wg.Done()
	log.Printf("🚀 worker %d démarré", id)

	for msg := range msgChan {
		start := time.Now()

		var user UserMsg
		if err := json.Unmarshal(msg.Value, &user); err != nil {
			log.Printf("worker %d json invalide, transfert brut", id)
			forwardRawMessage(producer, msg.Value)
			continue
		}

		fullName := strings.TrimSpace(user.Prenom + " " + user.Nom)

		email, err := lookupEmail(fullName)
		if err != nil {
			log.Printf("worker %d lookup error [%s]: %v", id, fullName, err)
			continue
		}

		enriched := EnrichedMsg{
			Nom:    user.Nom,
			Prenom: user.Prenom,
			Email:  email,
		}

		payload, err := json.Marshal(enriched)
		if err != nil {
			log.Printf("worker %d marshal error: %v", id, err)
			continue
		}

		topic := outTopic
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: payload,
		}, nil)

		if err != nil {
			log.Printf("worker %d kafka produce error: %v", id, err)
			continue
		}

		log.Printf("✅ worker %d traité %s en %v", id, fullName, time.Since(start))
	}
}

func main() {
	log.Println("🎯 H1 ConcurrentDBQueries")

	initDB()
	defer db.Close()

	bootstrap := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrap == "" {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS manquant")
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrap,
		"client.id":         "user-processor-h1",
	})
	if err != nil {
		log.Fatalf("producer error: %v", err)
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrap,
		"group.id":          "user-processor-group-h1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{inputTopic}, nil); err != nil {
		log.Fatalf("subscribe error: %v", err)
	}

	msgChan = make(chan *kafka.Message, bufferSize)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, producer)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("👂 consumer prêt")

	run := true
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("signal reçu: %v", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				msgChan <- e
			case kafka.Error:
				log.Printf("kafka error: %v", e)
			}
		}
	}

	close(msgChan)
	wg.Wait()
	producer.Flush(15000)
	log.Println("🛑 arrêt propre")
}