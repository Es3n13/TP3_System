package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"
    "sync"
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

// Cache in-memory: nom → email
var emailCache sync.Map
var cacheHits, cacheMisses, dbQueries int64

func main() {
    log.Println("Démarrage user-processor AVEC CACHE IN-MEMORY...")

    // Connexion DB (fallback pour cache miss)
    db, err := sql.Open("mysql", "root:pass@tcp(mysql:3306)/uqar")
    if err != nil {
        log.Fatal("Erreur connexion MySQL:", err)
    }
    defer db.Close()

    if err := db.Ping(); err != nil {
        log.Fatal("Impossible de joindre MySQL:", err)
    }
    log.Println("Connexion MySQL OK (utilisée seulement pour cache miss)")

    // Pré-warm cache ? (optionnel, commente si tu veux test pur)
    // warmCache(db)

    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  []string{"kafka-0.kafka:9092"},
        Topic:    "users",
        GroupID:  "user-processor-group",
        MinBytes: 10e3,                       // Batch lecture
        MaxBytes: 10e6,
    })
    defer reader.Close()

    writer := &kafka.Writer{
        Addr:     kafka.TCP("kafka-0.kafka:9092"),
        Topic:    "notification",
        Balancer: &kafka.LeastBytes{},
    }
    defer writer.Close()

    log.Println("Kafka reader/writer prêts (batch activé)")
    
    ticker := time.NewTicker(30 * time.Second) // Stats toutes les 30s
    defer ticker.Stop()

    go func() {
        for range ticker.C {
            total := cacheHits + cacheMisses
            hitRate := float64(cacheHits) / float64(total) * 100
            log.Printf("STATS CACHE: %d hits (%.1f%%), %d misses, %d DB queries | total: %d",
                cacheHits, hitRate, cacheMisses, dbQueries, total)
        }
    }()

    for {
        msg, err := reader.ReadMessage(context.Background())
        if err != nil {
            log.Println("Erreur lecture Kafka:", err)
            continue
        }

        raw := string(msg.Value)
        log.Printf(" Message reçu: %s", raw[:min(100, len(raw))]+"...")

        // Marqueur spécial
        if raw == "%%END%%" {
            log.Println(" Message %%END%% → forward")
            writer.WriteMessages(context.Background(), kafka.Message{Value: msg.Value})
            continue
        }

        var input InputUser
        if err := json.Unmarshal(msg.Value, &input); err != nil {
            log.Println("Ereur JSON:", err)
            continue
        }

        output := OutputUser{Nom: input.Nom}
        
        // 🔑 CACHE LOOKUP (hypothèse #1)
        if email, ok := emailCache.Load(input.Nom); ok {
            output.Email = email.(string)
            cacheHits++
            log.Printf("CACHE HIT pour %s → %s", input.Nom, output.Email)
        } else {
            // Cache miss → DB
            cacheMisses++
            var email string
            err := db.QueryRow("SELECT email FROM etudiants WHERE nom = ?", input.Nom).Scan(&email)
            if err == nil {
                emailCache.Store(input.Nom, email) // Stocke en cache
                output.Email = email
                log.Printf("CACHE MISS → DB HIT pour %s → %s (mis en cache)", input.Nom, email)
            } else {
                log.Printf("CACHE MISS → DB MISS pour %s", input.Nom)
                output.Email = ""
            }
            dbQueries++
        }

        // Envoi enrichi
        enriched, _ := json.Marshal(output)
        writer.WriteMessages(context.Background(), kafka.Message{Value: enriched})
        log.Printf("📤 Envoi: %s", string(enriched))
    }
}

// Warm cache au démarrage
func warmCache(db *sql.DB) {
    log.Println("🔥 Pré-warm cache...")
    rows, _ := db.Query("SELECT nom, email FROM etudiants LIMIT 1000")
    defer rows.Close()
    count := 0
    for rows.Next() {
        var nom, email string
        rows.Scan(&nom, &email)
        emailCache.Store(nom, email)
        count++
    }
    log.Printf("✅ Cache pré-rempli: %d entrées", count)
}

// Helper
func min(a, b int) int {
    if a < b { return a }
    return b
}