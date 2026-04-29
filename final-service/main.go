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

// Représente le format JSON d'entrée reçu depuis Kafka
// Exemple de message: {"nom": "Dupont"}
type InputUser struct {
    Nom string `json:"nom"`
}

// Représente le format JSON de sortie envoyé vers Kafka
type OutputUser struct {
    Nom   string `json:"nom"`
    Email string `json:"email,omitempty"`
}

// Tâche interne qui regroupe la clé Kafka et l'utilisateur décodé
type Task struct {
    Key  []byte
    User InputUser
}

// Compteur global du nombre de messages produits
var totalProduced atomic.Int64

// Constantes de configuration Kafka et traitement
const (
    inputTopic      = "users"                    // topic Kafka d'entrée
    outputTopic     = "notification"             // topic Kafka de sortie
    consumerGroupID = "user-processor-h4-group"  // groupe de consommateurs Kafka
    workerCount     = 8                          // nombre de workers concurrents
    bufferCapacity  = 1000                       // taille max du buffer avant traitement
)

func main() {
    log.Println("🏎️ User Processor V.Finale")

    // Variables pour mesurer le temps de traitement
    var startTime time.Time
    var firstMessageReceived bool

    // Récupération de la config Kafka par variable d'environnement, avec une valeur par défaut si non définie.
    kafkaServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafkaServers == "" {
        kafkaServers = "kafka-0.kafka:9092"
    }

    // Récupération de la config MySQL (DSN) par variable d'environnement.
    dsn := os.Getenv("MYSQL_DSN")
    if dsn == "" {
        dsn = "root:pass@tcp(mysql:3306)/uqar"
    }

    // Ouverture de la connexion à la base MySQL
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        log.Fatal("DB:", err)
    }
    defer db.Close()

    // Configuration du pool de connexions à la base
    db.SetMaxOpenConns(50)
    db.SetMaxIdleConns(25)

    // Création du producteur Kafka avec config de batching
    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers":  kafkaServers,
        "linger.ms":          10,
        "batch.num.messages": 10000,
    })
    if err != nil {
        log.Fatal("Producer:", err)
    }
    defer producer.Close()

    // Goroutine dédiée pour écouter les événements du producteur.
    go func() {
        for e := range producer.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    log.Printf("Échec de la livraison: %v", ev.TopicPartition.Error)
                }
            }
        }
    }()

    // Création du consommateur Kafka
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

    // Précharge la cache avec tous les utilisateurs présents en base
    warmupCache(db, userCache, &cacheMu)

    // Buffer de tâches à traiter en batch
    tasks := make([]Task, 0, bufferCapacity)
    var endKey []byte

    // Boucle principale de consommation Kafka
consumeLoop:
    for {
        // Timeout de 100 ms (retourne un événement ou nil)
        ev := consumer.Poll(100)
        if ev == nil {
            continue
        }
        switch e := ev.(type) {
        case *kafka.Message:

            if e.TopicPartition.Error != nil {
                continue
            }

            // Démarre le chrono à la réception du premier message
            if !firstMessageReceived {
                startTime = time.Now()
                firstMessageReceived = true
            }

            // Message spécial de fin: valeur "%%END%%"
            if string(e.Value) == "%%END%%" {
                endKey = e.Key
                break consumeLoop
            }

            // Décodage du JSON d'entrée en InputUser
            var input InputUser
            if err := json.Unmarshal(e.Value, &input); err != nil {
                // En cas de JSON invalide, on ignore ce message
                continue
            }

            // Ajoute la tâche au buffer
            tasks = append(tasks, Task{Key: e.Key, User: input})

            // Quand on atteint la capacité du buffer, on traite le batch
            if len(tasks) >= bufferCapacity {
                processMessages(tasks, db, producer, userCache, &cacheMu)
                tasks = tasks[:0]
            }
        }
    }

    // Traite les messages restants dans le buffer après la fin
    if len(tasks) > 0 {
        processMessages(tasks, db, producer, userCache, &cacheMu)
    }

    // Calcule la durée totale de traitement depuis le premier message
    duration := time.Since(startTime)

    // Envoie un message spécial "%%END%%" sur le topic de sortie pour signaler la fin du traitement.
    producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: stringPtr(outputTopic), Partition: kafka.PartitionAny},
        Key:            endKey,
        Value:          []byte("%%END%%"),
    }, nil)
    producer.Flush(5000)

    // Statistiques de performance
    total := totalProduced.Load()
    var msgPerSec float64
    if duration.Seconds() > 0 {
        msgPerSec = float64(total) / duration.Seconds()
    }

    log.Printf("User Processor V.Finale a terminé | total envoyé: %d", total)
    log.Printf("Temps de traitement: %v", duration)
    log.Printf("Performance: %.2f messages/sec", msgPerSec)
}

// Traite un batch de tâches:
// 1) complète la cache avec les noms manquants depuis la BD
// 2) distribue le travail sur plusieurs workers concurrents
// 3) produit les messages sur Kafka
func processMessages(tasks []Task, db *sql.DB, producer *kafka.Producer, cache map[string]string, mu *sync.RWMutex) {
    // Liste des noms qui ne sont pas encore dans le cache
    missing := make([]string, 0, len(tasks))
    mu.RLock()
    for _, t := range tasks {
        if _, ok := cache[t.User.Nom]; !ok {
            missing = append(missing, t.User.Nom)
        }
    }
    mu.RUnlock()

    // Si des noms manquent, on va les chercher une seule fois en BD
    if len(missing) > 0 {
        fillCache(db, cache, mu, missing)
    }

    // Curseur pour répartir les index des tâches entre les goroutines
    var cursor atomic.Uint64
    var wg sync.WaitGroup
    numTasks := uint64(len(tasks))

    // Lance workerCount goroutines pour traiter les tâches en parallèle
    for i := 0; i < workerCount; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for {
                // Récupère l'index suivant
                idx := cursor.Add(1) - 1
                if idx >= numTasks {
                    break
                }
                task := tasks[idx]

                // Récupère l'email depuis le cache en mémoire
                email := getEmailFromCache(cache, mu, task.User.Nom)

                // Construit le message Kafka de sortie (JSON nom + email)
                outMsg := createKafkaMessage(task.Key, task.User.Nom, email)

                // Envoie le message Kafka
                produceMessage(producer, outMsg)
            }
        }()
    }
    wg.Wait()
}

// Complète la cache
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

    // Requête SQL paramétrée avec un IN sur la liste de noms
    query := fmt.Sprintf(
        "SELECT nom, email FROM etudiants WHERE nom IN (%s)",
        strings.Join(placeholders, ","),
    )
    rows, err := db.Query(query, args...)
    if err != nil {
        log.Printf("Erreur lors du remplissage de la cache: %v", err)
        return
    }
    defer rows.Close()

    // On met à jour le cache
    mu.Lock()
    for rows.Next() {
        var nom, email string
        if err := rows.Scan(&nom, &email); err == nil {
            cache[nom] = email
        }
    }
    mu.Unlock()
}

// Lit un email dans le cache
func getEmailFromCache(cache map[string]string, mu *sync.RWMutex, nom string) string {
    mu.RLock()
    defer mu.RUnlock()
    return cache[nom]
}

// Construit un message Kafka de sortie à partir du nom et de l'email
func createKafkaMessage(key []byte, nom, email string) kafka.Message {
    out := OutputUser{Nom: nom, Email: email}
    val, _ := json.Marshal(out)
    return kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: stringPtr(outputTopic), Partition: kafka.PartitionAny},
        Key:            key,
        Value:          val,
    }
}

// Produit un message Kafka et met à jour le compteur global
func produceMessage(producer *kafka.Producer, msg kafka.Message) {
    if err := producer.Produce(&msg, nil); err != nil {
        log.Printf("Erreur lors de la production: %v", err)
    }
    totalProduced.Add(1)
}

// Précharge le cache avec tous les utilisateurs présents en BD.
func warmupCache(db *sql.DB, cache map[string]string, mu *sync.RWMutex) {
    log.Println("Warming up de la cache...")
    start := time.Now()

    rows, err := db.Query("SELECT nom, email FROM etudiants")
    if err != nil {
        log.Printf("Cache warmup error: %v", err)
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

    log.Printf("Cache warmed up: %d utilisateurs chargés en %v", count, time.Since(start))
}

func stringPtr(s string) *string {
    return &s
}