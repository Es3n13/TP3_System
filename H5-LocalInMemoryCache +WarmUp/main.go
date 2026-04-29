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

type Task struct {
    Key  []byte
    User InputUser
}

var totalProduced atomic.Int64

const (
    inputTopic      = "users"
    outputTopic     = "notification"
    consumerGroupID = "user-processor-h5-group"
    workerCount     = 8
    bufferCapacity  = 1000 
)

func main() {
    log.Println("🏎️ User Processor V.Finale")

    var startTime time.Time
    var firstMessageReceived bool

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

    db.SetMaxOpenConns(50)
    db.SetMaxIdleConns(25)

    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers":  kafkaServers,
        "linger.ms":          10,
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
                    log.Printf("Livraison échouée: %v", ev.TopicPartition.Error)
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

    userCache := make(map[string]string)
    var cacheMu sync.RWMutex

    warmupCache(db, userCache, &cacheMu)

    tasks := make([]Task, 0, bufferCapacity)
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
                continue
            }

            if !firstMessageReceived {
                startTime = time.Now()
                firstMessageReceived = true
            }
            if string(e.Value) == "%%END%%" {
                endKey = e.Key
                break consumeLoop
            }

            var input InputUser
            if err := json.Unmarshal(e.Value, &input); err != nil {
                continue
            }
            tasks = append(tasks, Task{Key: e.Key, User: input})
            if len(tasks) >= bufferCapacity {
                processH4(tasks, db, producer, userCache, &cacheMu)
                tasks = tasks[:0]
            }
        }
    }

    if len(tasks) > 0 {
        processH4(tasks, db, producer, userCache, &cacheMu)
    }

    duration := time.Since(startTime)

    log.Printf("Forwarding %%END%% to %s", outputTopic)
    producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: stringPtr(outputTopic), Partition: kafka.PartitionAny},
        Key:            endKey,
        Value:          []byte("%%END%%"),
    }, nil)
    producer.Flush(5000)

    total := totalProduced.Load()
    var msgPerSec float64
    if duration.Seconds() > 0 {
        msgPerSec = float64(total) / duration.Seconds()
    }

    log.Printf("✅ Le User Processor à terminé | total envoyé: %d", total)
    log.Printf("⏱️ Temps de traitement: %v", duration)
    log.Printf("🚀 Performance: %.2f messages/sec", msgPerSec)
}

func process(tasks []Task, db *sql.DB, producer *kafka.Producer, cache map[string]string, mu *sync.RWMutex) {
    missing := make([]string, 0, len(tasks))
    mu.RLock()
    for _, t := range tasks {
        if _, ok := cache[t.User.Nom]; !ok {
            missing = append(missing, t.User.Nom)
        }
    }
    mu.RUnlock()

    if len(missing) > 0 {
        fillCache(db, cache, mu, missing)
    }
    var cursor atomic.Uint64
    var wg sync.WaitGroup
    numTasks := uint64(len(tasks))

    for i := 0; i < workerCount; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                idx := cursor.Add(1) - 1
                if idx >= numTasks {
                    break
                }
                task := tasks[idx]
                email := getEmailFromCache(cache, mu, task.User.Nom)
                outMsg := createKafkaMessage(task.Key, task.User.Nom, email)
                produceMessage(producer, outMsg)
            }
        }()
    }
    wg.Wait()
}

func fillCache(db *sql.DB, cache map[string]string, mu *sync.RWMutex, names []string) {
    uniqueNames := make(map[string]struct{})
    for _, n := range names {
        uniqueNames[n] = struct{}{}
    }
    distinct := make([]string, 0, len(uniqueNames))
    for n := range uniqueNames {
        distinct = append(distinct, n)
    }

    placeholders := make([]string, len(distinct))
    args := make([]any, len(distinct))
    for i, n := range distinct {
        placeholders[i] = "?"
        args[i] = n
    }

    query := fmt.Sprintf("SELECT nom, email FROM etudiants WHERE nom IN (%s)", strings.Join(placeholders, ","))
    rows, err := db.Query(query, args...)
    if err != nil {
        log.Printf("Erreur de remplissage de la cache: %v", err)
        return
    }
    defer rows.Close()

    mu.Lock()
    for rows.Next() {
        var nom, email string
        if err := rows.Scan(&nom, &email); err == nil {
            cache[nom] = email
        }
    }
    mu.Unlock()
}

func warmupCache(db *sql.DB, cache map[string]string, mu *sync.RWMutex) {
    log.Println("🔥 Warming up de la cache...")
    start := time.Now()

    rows, err := db.Query("SELECT nom, email FROM etudiants")
    if err != nil {
        log.Printf("Erreur du cache warmup: %v", err)
        return
    }
    defer rows.Close()

    mu.Lock()
    count := 0
    for rows.Next() {
        var nom, email string
        if err := rows.Scan(&nom, &email); err == nil {
            cache[nom] = email
            count++
        }
    }
    mu.Unlock()

    log.Printf("✅ Cache warmed up: %d utilisateurs chargés en %v", count, time.Since(start))
}

func getEmailFromCache(cache map[string]string, mu *sync.RWMutex, nom string) string {
    mu.RLock()
    defer mu.RUnlock()
    return cache[nom]
}

func createKafkaMessage(key []byte, nom, email string) kafka.Message {
    out := OutputUser{Nom: nom, Email: email}
    val, _ := json.Marshal(out)
    return kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: stringPtr(outputTopic), Partition: kafka.PartitionAny},
        Key:            key,
        Value:          val,
    }
}
func produceMessage(producer *kafka.Producer, msg kafka.Message) {
    if err := producer.Produce(&msg, nil); err != nil {
        log.Printf("Erreur de production: %v", err)
    }
    totalProduced.Add(1)
}

func stringPtr(s string) *string {
    return &s
}