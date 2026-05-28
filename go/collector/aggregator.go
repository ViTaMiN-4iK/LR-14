package main

import (
	"sync"
	"time"
)

type WindowConfig struct {
	TumblingInterval time.Duration
	SlidingWindow    time.Duration
}

type AggregatedReading struct {
	PatientID     string     `json:"patient_id"`
	SensorType   SensorType `json:"sensor_type"`
	Timestamp    time.Time  `json:"timestamp"`
	WindowStart  time.Time  `json:"window_start"`
	WindowEnd    time.Time  `json:"window_end"`
	Count        int        `json:"count"`
	Avg          float64    `json:"avg"`
	Min          float64    `json:"min"`
	Max          float64    `json:"max"`
}

type BloodPressureAggregated struct {
	PatientID    string     `json:"patient_id"`
	SensorType   SensorType `json:"sensor_type"`
	Timestamp    time.Time  `json:"timestamp"`
	WindowStart  time.Time  `json:"window_start"`
	WindowEnd    time.Time  `json:"window_end"`
	Count        int        `json:"count"`
	SysAvg       float64    `json:"systolic_avg"`
	SysMin       float64    `json:"systolic_min"`
	SysMax       float64    `json:"systolic_max"`
	DiaAvg       float64    `json:"diastolic_avg"`
	DiaMin       float64    `json:"diastolic_min"`
	DiaMax       float64    `json:"diastolic_max"`
}

type Aggregator struct {
	config          WindowConfig
	tumblingBuffer  map[string][]float64
	slidingBuffers  map[string]*SlidingWindowBuffer
	bpBuffer        map[string][]BloodPressureReading
	outputCh        chan interface{}
	mu              sync.Mutex
}

type SlidingWindowBuffer struct {
	values    []float64
	timestamps []time.Time
	windowSize time.Duration
}

func NewSlidingWindowBuffer(windowSize time.Duration) *SlidingWindowBuffer {
	return &SlidingWindowBuffer{
		values:     make([]float64, 0),
		timestamps: make([]time.Time, 0),
		windowSize: windowSize,
	}
}

func (sb *SlidingWindowBuffer) Add(value float64, timestamp time.Time) {
	sb.values = append(sb.values, value)
	sb.timestamps = append(sb.timestamps, timestamp)
	sb.cleanOld(timestamp)
}

func (sb *SlidingWindowBuffer) cleanOld(timestamp time.Time) {
	cutoff := timestamp.Add(-sb.windowSize)
	newValues := make([]float64, 0)
	newTimestamps := make([]time.Time, 0)
	for i, t := range sb.timestamps {
		if t.After(cutoff) {
			newValues = append(newValues, sb.values[i])
			newTimestamps = append(newTimestamps, sb.timestamps[i])
		}
	}
	sb.values = newValues
	sb.timestamps = newTimestamps
}

func (sb *SlidingWindowBuffer) GetStats() (count int, avg, min, max float64) {
	if len(sb.values) == 0 {
		return 0, 0, 0, 0
	}
	count = len(sb.values)
	sum := 0.0
	min = sb.values[0]
	max = sb.values[0]
	for _, v := range sb.values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	avg = sum / float64(count)
	return
}

func NewAggregator(config WindowConfig, outputCh chan interface{}) *Aggregator {
	return &Aggregator{
		config:         config,
		tumblingBuffer: make(map[string][]float64),
		slidingBuffers: make(map[string]*SlidingWindowBuffer),
		bpBuffer:       make(map[string][]BloodPressureReading),
		outputCh:       outputCh,
	}
}

func (a *Aggregator) AddReading(reading Reading) {
	key := reading.PatientID + "_" + string(reading.SensorType)

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.slidingBuffers[key]; !exists {
		a.slidingBuffers[key] = NewSlidingWindowBuffer(a.config.SlidingWindow)
	}

	a.slidingBuffers[key].Add(reading.Value, reading.Timestamp)

	if a.tumblingBuffer[key] == nil {
		a.tumblingBuffer[key] = make([]float64, 0)
	}
	a.tumblingBuffer[key] = append(a.tumblingBuffer[key], reading.Value)
}

func (a *Aggregator) AddBloodPressure(bp BloodPressureReading) {
	key := bp.PatientID + "_" + string(bp.SensorType)

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.bpBuffer[key] == nil {
		a.bpBuffer[key] = make([]BloodPressureReading, 0)
	}
	a.bpBuffer[key] = append(a.bpBuffer[key], bp)
}

func (a *Aggregator) StartTumblingWindow(ctx <-chan struct{}) {
	ticker := time.NewTicker(a.config.TumblingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx:
			a.flushTumblingBuffers()
			return
		case <-ticker.C:
			a.flushTumblingBuffers()
		}
	}
}

func (a *Aggregator) flushTumblingBuffers() {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	windowEnd := now
	windowStart := now.Add(-a.config.TumblingInterval)

	for key, values := range a.tumblingBuffer {
		if len(values) == 0 {
			continue
		}

		parts := splitKey(key)
		if len(parts) != 2 {
			continue
		}
		patientID := parts[0]
		sensorType := SensorType(parts[1])

		stats := calculateStats(values)

		agg := AggregatedReading{
			PatientID:    patientID,
			SensorType:   sensorType,
			Timestamp:    now,
			WindowStart:  windowStart,
			WindowEnd:    windowEnd,
			Count:        len(values),
			Avg:          stats.avg,
			Min:          stats.min,
			Max:          stats.max,
		}

		select {
		case a.outputCh <- agg:
		default:
		}
	}

	a.tumblingBuffer = make(map[string][]float64)
}

func (a *Aggregator) GetSlidingStats(patientID string, sensorType SensorType) (count int, avg, min, max float64) {
	key := patientID + "_" + string(sensorType)

	a.mu.Lock()
	defer a.mu.Unlock()

	if buf, exists := a.slidingBuffers[key]; exists {
		return buf.GetStats()
	}
	return 0, 0, 0, 0
}

func (a *Aggregator) StartBPWindow(ctx <-chan struct{}) {
	ticker := time.NewTicker(a.config.TumblingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx:
			a.flushBPBuffers()
			return
		case <-ticker.C:
			a.flushBPBuffers()
		}
	}
}

func (a *Aggregator) flushBPBuffers() {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	windowEnd := now
	windowStart := now.Add(-a.config.TumblingInterval)

	for key, readings := range a.bpBuffer {
		if len(readings) == 0 {
			continue
		}

		parts := splitKey(key)
		if len(parts) != 2 {
			continue
		}
		patientID := parts[0]

		var sysVals, diaVals []float64
		for _, r := range readings {
			sysVals = append(sysVals, r.Systolic)
			diaVals = append(diaVals, r.Diastolic)
		}

		sysStats := calculateStats(sysVals)
		diaStats := calculateStats(diaVals)

		agg := BloodPressureAggregated{
			PatientID:   patientID,
			SensorType:  BloodPressure,
			Timestamp:   now,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Count:       len(readings),
			SysAvg:      sysStats.avg,
			SysMin:      sysStats.min,
			SysMax:      sysStats.max,
			DiaAvg:      diaStats.avg,
			DiaMin:      diaStats.min,
			DiaMax:      diaStats.max,
		}

		select {
		case a.outputCh <- agg:
		default:
		}
	}

	a.bpBuffer = make(map[string][]BloodPressureReading)
}

func splitKey(key string) []string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '_' {
			return []string{key[:i], key[i+1:]}
		}
	}
	return nil
}

type stats struct {
	avg, min, max float64
}

func calculateStats(values []float64) stats {
	if len(values) == 0 {
		return stats{}
	}
	sum := 0.0
	min := values[0]
	max := values[0]
	for _, v := range values {
		sum += v
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return stats{
		avg: sum / float64(len(values)),
		min: min,
		max: max,
	}
}
