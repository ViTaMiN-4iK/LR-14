//go:build !nogorust && windows && cgo

package main

/*
#cgo LDFLAGS: -LD:/projects/lab 14/rust/validator/target/release -lmedical_validator
#include <stdlib.h>
#include "medical_validator.h"
*/
import "C"
import (
	"unsafe"
)

type ValidationResult struct {
	IsValid      bool
	ErrorCode    int32
	ErrorMessage string
}

func validatePulse(value float64) ValidationResult {
	cValue := C.double(value)
	result := C.validate_pulse(cValue)
	return convertResult(result)
}

func validateSystolic(value float64) ValidationResult {
	cValue := C.double(value)
	result := C.validate_systolic(cValue)
	return convertResult(result)
}

func validateDiastolic(value float64) ValidationResult {
	cValue := C.double(value)
	result := C.validate_diastolic(cValue)
	return convertResult(result)
}

func validateTemperature(value float64) ValidationResult {
	cValue := C.double(value)
	result := C.validate_temperature(cValue)
	return convertResult(result)
}

func validateSpO2(value float64) ValidationResult {
	cValue := C.double(value)
	result := C.validate_spo2(cValue)
	return convertResult(result)
}

func convertResult(result C.ValidationResult) ValidationResult {
	var msg string
	if result.error_message != nil {
		msg = C.GoString(result.error_message)
		C.free_validation_result(result)
	}
	return ValidationResult{
		IsValid:      bool(result.is_valid),
		ErrorCode:    int32(result.error_code),
		ErrorMessage: msg,
	}
}
