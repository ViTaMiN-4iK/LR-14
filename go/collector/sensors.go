package main

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type SensorType string

const (
	Pulse         SensorType = "pulse"
	BloodPressure SensorType = "blood_pressure"
	Temperature   SensorType = "temperature"
	SpO2          SensorType = "spo2"
)

type Reading struct {
	PatientID  string     `json:"patient_id"`
	SensorType SensorType `json:"sensor_type"`
	Value      float64    `json:"value"`
	Timestamp  time.Time  `json:"timestamp"`
}

type BloodPressureReading struct {
	PatientID  string     `json:"patient_id"`
	SensorType SensorType `json:"sensor_type"`
	Systolic   float64    `json:"systolic"`
	Diastolic  float64    `json:"diastolic"`
	Timestamp  time.Time  `json:"timestamp"`
}

var patientIDs = []string{"P001", "P002", "P003", "P004", "P005"}

type Emulator struct {
	sensors []SensorType
	dataCh  chan interface{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func NewEmulator(sensors []SensorType, dataCh chan interface{}) *Emulator {
	return &Emulator{
		sensors: sensors,
		dataCh:  dataCh,
		stopCh:  make(chan struct{}),
	}
}

func (e *Emulator) Start() {
	for _, sensor := range e.sensors {
		e.wg.Add(1)
		go e.runSensor(sensor)
	}
}

func (e *Emulator) Stop() {
	close(e.stopCh)
	e.wg.Wait()
}

func (e *Emulator) runSensor(sensorType SensorType) {
	defer e.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			reading := e.generateReading(sensorType)
			select {
			case e.dataCh <- reading:
			default:
			}
		}
	}
}

func (e *Emulator) generateReading(sensorType SensorType) interface{} {
	patientID := patientIDs[rand.Intn(len(patientIDs))]
	now := time.Now()

	switch sensorType {
	case Pulse:
		base := 70.0 + rand.Float64()*20
		if rand.Float64() < 0.05 {
			base += rand.Float64() * 50
		}
		return Reading{
			PatientID:  patientID,
			SensorType: sensorType,
			Value:      math.Round(base*10) / 10,
			Timestamp:  now,
		}
	case BloodPressure:
		baseSys := 110.0 + rand.Float64()*30
		baseDia := 70.0 + rand.Float64()*20
		if rand.Float64() < 0.03 {
			baseSys += rand.Float64() * 50
			baseDia += rand.Float64() * 30
		}
		return BloodPressureReading{
			PatientID:  patientID,
			SensorType: sensorType,
			Systolic:   math.Round(baseSys),
			Diastolic:  math.Round(baseDia),
			Timestamp:  now,
		}
	case Temperature:
		base := 36.4 + rand.Float64()*0.8
		if rand.Float64() < 0.02 {
			base += rand.Float64() * 1.5
		}
		return Reading{
			PatientID:  patientID,
			SensorType: sensorType,
			Value:      math.Round(base*100) / 100,
			Timestamp:  now,
		}
	case SpO2:
		base := 96.0 + rand.Float64()*3
		if rand.Float64() < 0.02 {
			base -= rand.Float64() * 10
		}
		return Reading{
			PatientID:  patientID,
			SensorType: sensorType,
			Value:      math.Round(base*10) / 10,
			Timestamp:  now,
		}
	default:
		return nil
	}
}
