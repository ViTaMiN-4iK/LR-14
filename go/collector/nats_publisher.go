package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type NATSPublisher struct {
	conn   *nats.Conn
	js     nats.JetStreamContext
	stream string
	subject string
}

func NewNATSPublisher(natsURL string) (*NATSPublisher, error) {
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	publisher := &NATSPublisher{
		conn:    conn,
		js:      js,
		stream:  "MEDICAL_DATA",
		subject: "medical.readings",
	}

	if err := publisher.setupStream(); err != nil {
		log.Printf("Warning: Failed to setup stream: %v", err)
	}

	return publisher, nil
}

func (p *NATSPublisher) setupStream() error {
	streamInfo, err := p.js.StreamInfo(p.stream)
	if err == nats.ErrStreamNotFound {
		_, err = p.js.AddStream(&nats.StreamConfig{
			Name:        p.stream,
			Subjects:    []string{p.subject + ".>"},
			Retention:   nats.InterestPolicy,
			MaxAge:      24 * time.Hour,
			Storage:     nats.MemoryStorage,
		})
		if err != nil {
			return fmt.Errorf("failed to create stream: %w", err)
		}
		log.Printf("Created NATS stream: %s", p.stream)
	} else if err != nil {
		return fmt.Errorf("failed to get stream info: %w", err)
	} else {
		log.Printf("Using existing NATS stream: %s (created: %v)", p.stream, streamInfo.Created)
	}

	return nil
}

func (p *NATSPublisher) Publish(data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := &nats.Msg{
		Subject: p.subject,
		Data:    jsonData,
	}

	_, err = p.js.PublishMsg(msg)
	return err
}

func (p *NATSPublisher) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

func RunNATSExample() {
	fmt.Println("=== NATS Publisher Example ===")
	fmt.Println("This module publishes medical data to NATS JetStream")

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	publisher, err := NewNATSPublisher(natsURL)
	if err != nil {
		log.Printf("Failed to create publisher: %v", err)
		fmt.Println("NATS not available. Data will be printed to stdout instead.")
		fmt.Println("To enable NATS, start a NATS server:")
		fmt.Println("  docker run -p 4222:4222 nats:latest")
		return
	}
	defer publisher.Close()

	fmt.Printf("Connected to NATS at %s\n", natsURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		count := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				data := map[string]interface{}{
					"patient_id":  fmt.Sprintf("P%03d", count%5+1),
					"sensor_type": []string{"pulse", "temperature", "spo2", "blood_pressure"}[count%4],
					"value":       70.0 + float64(count%50),
					"timestamp":   time.Now().Unix(),
				}

				if err := publisher.Publish(data); err != nil {
					log.Printf("Failed to publish: %v", err)
				} else {
					count++
					log.Printf("Published reading #%d", count)
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Publishing to NATS. Press Ctrl+C to stop.")
	<-sigCh
	fmt.Println("\nShutting down...")
}
