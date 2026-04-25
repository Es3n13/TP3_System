package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"

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
func main() {
    log.Println("Démarrage du user-processor...")

    db, err := sql.Open("mysql", "root:pass@tcp(mysql:3306)/uqar")
    if err != nil {
        log.Fatal("Erreur connexion MySQL:", err)
    }
    defer db.Close()

    if err := db.Ping(); err != nil {
        log.Fatal("Impossible de joindre MySQL:", err)
    }
    log.Println("Connexion MySQL OK")
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"kafka-0.kafka:9092"},
        Topic:   "users",
        GroupID: "user-processor-group",
    })
    defer reader.Close()

    writer := &kafka.Writer{
        Addr:     kafka.TCP("kafka-0.kafka:9092"),
        Topic:    "notification",
        Balancer: &kafka.LeastBytes{},
    }
    defer writer.Close()

    log.Println("Kafka reader/writer prêts")
    for {
        msg, err := reader.ReadMessage(context.Background())
        if err != nil {
            log.Println("Erreur lecture Kafka:", err)
            continue
        }

        raw := string(msg.Value)
        log.Printf("Message reçu: %s\n", raw)

        if raw == "%%END%%" {
            log.Println("⚠ Message non JSON → renvoyé tel quel")
            err = writer.WriteMessages(context.Background(), kafka.Message{
                Value: msg.Value,
            })
            if err != nil {
                log.Println("Erreur envoi du marqueur %%END%%:", err)
            }
            continue
        }

        var input InputUser
        err = json.Unmarshal(msg.Value, &input)
        if err != nil {
            log.Println("Erreur JSON:", err)
            continue
        }

        log.Printf("Nom reçu: %s\n", input.Nom)
        output := OutputUser{
            Nom: input.Nom,
        }

        err = db.QueryRow("SELECT email FROM etudiants WHERE nom = ?", input.Nom).Scan(&output.Email)
        if err != nil {
            if err == sql.ErrNoRows {
                log.Printf("Aucun email trouvé pour: %s\n", input.Nom)
                output.Email = ""
            } else {
                log.Printf("Erreur SQL pour %s: %v\n", input.Nom, err)
                output.Email = ""
            }
        } else {
            log.Printf("Email trouvé: %s\n", output.Email)
        }

        enrichedMessage, err := json.Marshal(output)
        if err != nil {
            log.Println("Erreur encodage JSON:", err)
            continue
        }

        err = writer.WriteMessages(context.Background(), kafka.Message{
            Value: enrichedMessage,
        })
        if err != nil {
            log.Println("Erreur envoi Kafka notification:", err)
            continue
        }

        log.Printf("Message envoyé: %s\n", string(enrichedMessage))
    }
}