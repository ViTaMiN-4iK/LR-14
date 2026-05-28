package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	fmt.Println("=== Medical Data Collector (Go) ===")
	fmt.Println("Starting health sensors emulator with window aggregation...")

	dataCh := make(chan interface{}, 1000)
	aggCh := make(chan interface{}, 100)
	outputCh := make(chan string, 100)

	emulator := NewEmulator([]SensorType{
		Pulse,
		BloodPressure,
		Temperature,
		SpO2,
	}, dataCh)

	aggConfig := WindowConfig{
		TumblingInterval: 5 * time.Second,
		SlidingWindow:    30 * time.Second,
	}
	aggregator := NewAggregator(aggConfig, aggCh)

	ctx, cancel := context.WithCancel(context.Background())

	emulator.Start()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		for data := range dataCh {
			switch d := data.(type) {
			case Reading:
				aggregator.AddReading(d)
				validateAndConvert(d, outputCh)
			case BloodPressureReading:
				aggregator.AddBloodPressure(d)
				validateBPAndConvert(d, outputCh)
			}
		}
	}()

	go func() {
		defer wg.Done()
		aggregator.StartTumblingWindow(ctx.Done())
	}()

	go func() {
		defer wg.Done()
		aggregator.StartBPWindow(ctx.Done())
	}()

	go processOutput(aggCh, outputCh)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Collector running. Press Ctrl+C to stop.")
	<-sigCh

	fmt.Println("\nShutting down gracefully...")
	cancel()
	emulator.Stop()
	close(dataCh)
	wg.Wait()
	close(aggCh)
	close(outputCh)

	fmt.Println("Collector stopped.")
}

func processOutput(aggCh <-chan interface{}, outputCh <-chan string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	aggCount := 0
	outputCount := 0

	for {
		select {
		case data, ok := <-aggCh:
			if !ok {
				return
			}
			aggCount++
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("JSON error: %v", err)
				continue
			}
			fmt.Printf("[AGG] %s\n", string(jsonData))
		case data, ok := <-outputCh:
			if !ok {
				return
			}
			outputCount++
			fmt.Printf("[RAW] %s\n", data)
		case <-ticker.C:
			fmt.Printf("\n--- Stats: %d aggregated, %d raw readings ---\n\n", aggCount, outputCount)
			aggCount = 0
			outputCount = 0
		}
	}
}

func validateAndConvert(r Reading, outputCh chan<- string) {
	var result ValidationResult
	switch r.SensorType {
	case Pulse:
		result = validatePulse(r.Value)
	case Temperature:
		result = validateTemperature(r.Value)
	case SpO2:
		result = validateSpO2(r.Value)
	}

	data := map[string]interface{}{
		"patient_id":  r.PatientID,
		"sensor_type": string(r.SensorType),
		"value":       r.Value,
		"timestamp":   r.Timestamp.Unix(),
		"is_valid":   result.IsValid,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	select {
	case outputCh <- string(jsonData):
	default:
	}
}

func validateBPAndConvert(bp BloodPressureReading, outputCh chan<- string) {
	sysResult := validateSystolic(bp.Systolic)
	diaResult := validateDiastolic(bp.Diastolic)

	data := map[string]interface{}{
		"patient_id":  bp.PatientID,
		"sensor_type": string(bp.SensorType),
		"systolic":    bp.Systolic,
		"diastolic":   bp.Diastolic,
		"timestamp":   bp.Timestamp.Unix(),
		"is_valid":    sysResult.IsValid && diaResult.IsValid,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	select {
	case outputCh <- string(jsonData):
	default:
	}
}
