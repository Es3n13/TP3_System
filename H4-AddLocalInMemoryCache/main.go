package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

type InputUser struct {
	Nom string `json:"nom"`
}

type OutputUser struct {
	Nom   string `json:"nom"`
	Email string `json:"email,omitempty"`
}

var (
	emailCache  sync.Map
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
	dbQueries   atomic.Int64
)

func main() {
	log.Println("Démarrage user-processor H2 (concurrent + cache in-memory)")

	mysqlDSN := getenv("MYSQL_DSN", "root:pass@tcp(mysql:3306)/uqar")
	kafkaBroker := getenv("KAFKA_BROKER", "kafka-0.kafka:9092")
	inputTopic := getenv("KAFKA_INPUT_TOPIC", "users")
	outputTopic := getenv("KAFKA_OUTPUT_TOPIC", "notification")
	groupID := getenv("KAFKA_GROUP_ID", "user-processor-h2-group")

	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatal("Erreur connexion MySQL: ", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatal("Impossible de joindre MySQL: ", err)
	}
	log.Println("Connexion MySQL OK")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          inputTopic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    outputTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	jobs := make(chan kafka.Message, 200)
	workerCount := 10

	for i := 0; i < workerCount; i++ {
		go worker(i, db, writer, jobs)
	}

	go logStats()

	log.Printf("Kafka prêt, %d workers démarrés", workerCount)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Erreur lecture Kafka:", err)
			continue
		}
		jobs <- msg
	}
}

func worker(id int, db *sql.DB, writer *kafka.Writer, jobs <-chan kafka.Message) {
	log.Printf("Worker %d démarré", id)

	for msg := range jobs {
		raw := string(msg.Value)

		if raw == "%%END%%" {
			log.Printf("Worker %d forward %%END%%", id)
			if err := writer.WriteMessages(context.Background(), kafka.Message{Value: msg.Value}); err != nil {
				log.Printf("Worker %d erreur write END: %v", id, err)
			}
			continue
		}

		var input InputUser
		if err := json.Unmarshal(msg.Value, &input); err != nil {
			log.Printf("Worker %d erreur JSON: %v", id, err)
			continue
		}

		start := time.Now()
		output := OutputUser{Nom: input.Nom}

		if email, ok := emailCache.Load(input.Nom); ok {
			output.Email = email.(string)
			cacheHits.Add(1)
			log.Printf("Worker %d CACHE HIT %s", id, input.Nom)
		} else {
			cacheMisses.Add(1)

			var email string
			err := db.QueryRow("SELECT email FROM etudiants WHERE nom = ?", input.Nom).Scan(&email)
			dbQueries.Add(1)

			if err == nil {
				emailCache.Store(input.Nom, email)
				output.Email = email
				log.Printf("Worker %d CACHE MISS -> DB HIT %s", id, input.Nom)
			} else {
				log.Printf("Worker %d CACHE MISS -> DB MISS %s : %v", id, input.Nom, err)
			}
		}

		enriched, err := json.Marshal(output)
		if err != nil {
			log.Printf("Worker %d erreur marshal: %v", id, err)
			continue
		}

		if err := writer.WriteMessages(context.Background(), kafka.Message{Value: enriched}); err != nil {
			log.Printf("Worker %d erreur write Kafka: %v", id, err)
			continue
		}

		log.Printf("Worker %d traité %s en %s", id, input.Nom, time.Since(start))
	}
}

func logStats() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		hits := cacheHits.Load()
		misses := cacheMisses.Load()
		dbq := dbQueries.Load()
		total := hits + misses

		hitRate := 0.0
		if total > 0 {
			hitRate = float64(hits) / float64(total) * 100
		}

		log.Printf("STATS CACHE | hits=%d misses=%d hit-rate=%.1f%% db-queries=%d total=%d",
			hits, misses, hitRate, dbq, total)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}