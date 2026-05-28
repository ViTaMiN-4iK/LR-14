//go:build nogorust || (!windows && !cgo)

package main

type ValidationResult struct {
	IsValid      bool
	ErrorCode    int32
	ErrorMessage string
}

const (
	PulseMin        = 30.0
	PulseMax        = 220.0
	SystolicMin     = 60.0
	SystolicMax     = 250.0
	DiastolicMin    = 40.0
	DiastolicMax    = 150.0
	TemperatureMin  = 35.0
	TemperatureMax  = 42.0
	SpO2Min         = 70.0
	SpO2Max         = 100.0
)

func validatePulse(value float64) ValidationResult {
	if value >= PulseMin && value <= PulseMax {
		return ValidationResult{IsValid: true, ErrorCode: 0}
	}
	return ValidationResult{
		IsValid:      false,
		ErrorCode:    1,
		ErrorMessage: "Pulse outside valid range",
	}
}

func validateSystolic(value float64) ValidationResult {
	if value >= SystolicMin && value <= SystolicMax {
		return ValidationResult{IsValid: true, ErrorCode: 0}
	}
	return ValidationResult{
		IsValid:      false,
		ErrorCode:    2,
		ErrorMessage: "Systolic pressure outside valid range",
	}
}

func validateDiastolic(value float64) ValidationResult {
	if value >= DiastolicMin && value <= DiastolicMax {
		return ValidationResult{IsValid: true, ErrorCode: 0}
	}
	return ValidationResult{
		IsValid:      false,
		ErrorCode:    3,
		ErrorMessage: "Diastolic pressure outside valid range",
	}
}

func validateTemperature(value float64) ValidationResult {
	if value >= TemperatureMin && value <= TemperatureMax {
		return ValidationResult{IsValid: true, ErrorCode: 0}
	}
	return ValidationResult{
		IsValid:      false,
		ErrorCode:    4,
		ErrorMessage: "Temperature outside valid range",
	}
}

func validateSpO2(value float64) ValidationResult {
	if value >= SpO2Min && value <= SpO2Max {
		return ValidationResult{IsValid: true, ErrorCode: 0}
	}
	return ValidationResult{
		IsValid:      false,
		ErrorCode:    5,
		ErrorMessage: "SpO2 outside valid range",
	}
}
