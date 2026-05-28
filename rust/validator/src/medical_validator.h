/**
 * Medical Validator - C API Header
 *
 * This header file provides the C-compatible interface for the Rust validator library.
 * It is used by Go code via cgo to call Rust validation functions.
 */

#ifndef MEDICAL_VALIDATOR_H
#define MEDICAL_VALIDATOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

/**
 * Validation result structure
 */
typedef struct {
    bool is_valid;
    bool is_normal;
    const char* status;
    const char* message;
} ValidationResult;

/**
 * Pulse validation
 * @param pulse Pulse value in bpm
 * @return ValidationResult with validation status
 */
ValidationResult validate_pulse(double pulse);

/**
 * Systolic blood pressure validation
 * @param systolic Systolic pressure in mmHg
 * @return ValidationResult with validation status
 */
ValidationResult validate_systolic(double systolic);

/**
 * Diastolic blood pressure validation
 * @param diastolic Diastolic pressure in mmHg
 * @return ValidationResult with validation status
 */
ValidationResult validate_diastolic(double diastolic);

/**
 * Temperature validation
 * @param temp Temperature in Celsius
 * @return ValidationResult with validation status
 */
ValidationResult validate_temperature(double temp);

/**
 * SpO2 (blood oxygen saturation) validation
 * @param spo2 SpO2 percentage (0-100)
 * @return ValidationResult with validation status
 */
ValidationResult validate_spo2(double spo2);

/**
 * Blood pressure combined validation
 * @param systolic Systolic pressure in mmHg
 * @param diastolic Diastolic pressure in mmHg
 * @return ValidationResult with combined validation status
 */
ValidationResult validate_blood_pressure(double systolic, double diastolic);

/**
 * Get the validation range for a parameter
 * @param param_name Parameter name (pulse, systolic, diastolic, temperature, spo2)
 * @param min Pointer to store minimum value
 * @param max Pointer to store maximum value
 * @return true if parameter found, false otherwise
 */
bool get_validation_range(const char* param_name, double* min, double* max);

/**
 * Initialize the validator library
 * @return true if initialization successful
 */
bool validator_init(void);

/**
 * Shutdown the validator library
 */
void validator_shutdown(void);

/* Validation range constants */
#define PULSE_MIN 30.0
#define PULSE_MAX 220.0
#define PULSE_NORMAL_MIN 60.0
#define PULSE_NORMAL_MAX 100.0

#define SYSTOLIC_MIN 60.0
#define SYSTOLIC_MAX 250.0
#define SYSTOLIC_NORMAL_MIN 90.0
#define SYSTOLIC_NORMAL_MAX 140.0

#define DIASTOLIC_MIN 40.0
#define DIASTOLIC_MAX 150.0
#define DIASTOLIC_NORMAL_MIN 60.0
#define DIASTOLIC_NORMAL_MAX 90.0

#define TEMPERATURE_MIN 35.0
#define TEMPERATURE_MAX 42.0
#define TEMPERATURE_NORMAL_MIN 36.1
#define TEMPERATURE_NORMAL_MAX 37.2

#define SPO2_MIN 70.0
#define SPO2_MAX 100.0
#define SPO2_NORMAL_MIN 95.0
#define SPO2_NORMAL_MAX 100.0

#ifdef __cplusplus
}
#endif

#endif /* MEDICAL_VALIDATOR_H */
