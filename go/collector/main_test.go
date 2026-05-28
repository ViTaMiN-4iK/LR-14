//go:build nogorust || (!windows && !cgo)

package main

import (
	"testing"
	"time"
)

func TestSensorTypes(t *testing.T) {
	sensors := []SensorType{Pulse, BloodPressure, Temperature, SpO2}

	if len(sensors) != 4 {
		t.Errorf("Expected 4 sensor types, got %d", len(sensors))
	}

	expected := map[SensorType]bool{
		Pulse:         true,
		BloodPressure: true,
		Temperature:   true,
		SpO2:          true,
	}

	for _, s := range sensors {
		if !expected[s] {
			t.Errorf("Unexpected sensor type: %s", s)
		}
	}
}

func TestEmulatorCreation(t *testing.T) {
	dataCh := make(chan interface{}, 100)
	emulator := NewEmulator([]SensorType{Pulse, Temperature}, dataCh)

	if emulator == nil {
		t.Fatal("Failed to create emulator")
	}

	if len(emulator.sensors) != 2 {
		t.Errorf("Expected 2 sensors, got %d", len(emulator.sensors))
	}

	if emulator.dataCh != dataCh {
		t.Error("Data channel mismatch")
	}
}

func TestWindowConfig(t *testing.T) {
	config := WindowConfig{
		TumblingInterval: 5 * time.Second,
		SlidingWindow:   30 * time.Second,
	}

	if config.TumblingInterval != 5*time.Second {
		t.Errorf("Expected tumbling interval 5s, got %v", config.TumblingInterval)
	}

	if config.SlidingWindow != 30*time.Second {
		t.Errorf("Expected sliding window 30s, got %v", config.SlidingWindow)
	}
}

func TestAggregatorCreation(t *testing.T) {
	config := WindowConfig{
		TumblingInterval: 5 * time.Second,
		SlidingWindow:   30 * time.Second,
	}
	outputCh := make(chan interface{}, 100)

	aggregator := NewAggregator(config, outputCh)

	if aggregator == nil {
		t.Fatal("Failed to create aggregator")
	}

	if aggregator.config.TumblingInterval != 5*time.Second {
		t.Error("Tumbling interval mismatch")
	}
}

func TestAddReading(t *testing.T) {
	config := WindowConfig{
		TumblingInterval: 5 * time.Second,
		SlidingWindow:   30 * time.Second,
	}
	outputCh := make(chan interface{}, 100)

	aggregator := NewAggregator(config, outputCh)

	reading := Reading{
		PatientID:  "P001",
		SensorType: Pulse,
		Value:      75.0,
		Timestamp: time.Now(),
	}

	aggregator.AddReading(reading)

	key := "P001_pulse"
	if _, exists := aggregator.slidingBuffers[key]; !exists {
		t.Error("Sliding buffer not created for reading")
	}
}

func TestSlidingWindowStats(t *testing.T) {
	config := WindowConfig{
		TumblingInterval: 5 * time.Second,
		SlidingWindow:   30 * time.Second,
	}
	outputCh := make(chan interface{}, 100)

	aggregator := NewAggregator(config, outputCh)

	for i := 0; i < 5; i++ {
		reading := Reading{
			PatientID:  "P001",
			SensorType: Pulse,
			Value:      float64(60 + i*10),
			Timestamp:  time.Now(),
		}
		aggregator.AddReading(reading)
	}

	count, avg, min, max := aggregator.GetSlidingStats("P001", Pulse)

	if count != 5 {
		t.Errorf("Expected count 5, got %d", count)
	}

	if avg != 80.0 {
		t.Errorf("Expected avg 80.0, got %f", avg)
	}

	if min != 60.0 {
		t.Errorf("Expected min 60.0, got %f", min)
	}

	if max != 100.0 {
		t.Errorf("Expected max 100.0, got %f", max)
	}
}

func TestValidationRanges(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		sensorType SensorType
		expected  bool
	}{
		{"Valid pulse", 75.0, Pulse, true},
		{"Low pulse", 25.0, Pulse, false},
		{"High pulse", 250.0, Pulse, false},
		{"Valid temp", 36.8, Temperature, true},
		{"Low temp", 34.0, Temperature, false},
		{"High temp", 43.0, Temperature, false},
		{"Valid spo2", 98.0, SpO2, true},
		{"Low spo2", 65.0, SpO2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result ValidationResult

			switch tt.sensorType {
			case Pulse:
				result = validatePulse(tt.value)
			case Temperature:
				result = validateTemperature(tt.value)
			case SpO2:
				result = validateSpO2(tt.value)
			}

			if result.IsValid != tt.expected {
				t.Errorf("validate(%s, %f) = %v, expected %v", tt.sensorType, tt.value, result.IsValid, tt.expected)
			}
		})
	}
}

func TestBloodPressureValidation(t *testing.T) {
	tests := []struct {
		name     string
		systolic float64
		diastolic float64
		sysExpected bool
		diaExpected bool
	}{
		{"Valid BP", 120.0, 80.0, true, true},
		{"Low systolic", 55.0, 80.0, false, true},
		{"High systolic", 260.0, 80.0, false, true},
		{"Low diastolic", 120.0, 35.0, true, false},
		{"High diastolic", 120.0, 160.0, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sysResult := validateSystolic(tt.systolic)
			diaResult := validateDiastolic(tt.diastolic)

			if sysResult.IsValid != tt.sysExpected {
				t.Errorf("validateSystolic(%f) = %v, expected %v", tt.systolic, sysResult.IsValid, tt.sysExpected)
			}
			if diaResult.IsValid != tt.diaExpected {
				t.Errorf("validateDiastolic(%f) = %v, expected %v", tt.diastolic, diaResult.IsValid, tt.diaExpected)
			}
		})
	}
}

func TestValidationConstants(t *testing.T) {
	if PulseMin != 30.0 || PulseMax != 220.0 {
		t.Errorf("Pulse range incorrect: %v-%v", PulseMin, PulseMax)
	}
	if SystolicMin != 60.0 || SystolicMax != 250.0 {
		t.Errorf("Systolic range incorrect: %v-%v", SystolicMin, SystolicMax)
	}
	if DiastolicMin != 40.0 || DiastolicMax != 150.0 {
		t.Errorf("Diastolic range incorrect: %v-%v", DiastolicMin, DiastolicMax)
	}
	if TemperatureMin != 35.0 || TemperatureMax != 42.0 {
		t.Errorf("Temperature range incorrect: %v-%v", TemperatureMin, TemperatureMax)
	}
	if SpO2Min != 70.0 || SpO2Max != 100.0 {
		t.Errorf("SpO2 range incorrect: %v-%v", SpO2Min, SpO2Max)
	}
}
