use std::ffi::{CStr, CString};
use std::os::raw::c_char;

#[repr(C)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub error_code: i32,
    pub error_message: *mut c_char,
}

impl Drop for ValidationResult {
    fn drop(&mut self) {
        if !self.error_message.is_null() {
            unsafe {
                CString::from_raw(self.error_message);
            }
        }
    }
}

const PULSE_MIN: f64 = 30.0;
const PULSE_MAX: f64 = 220.0;

const SYSTOLIC_MIN: f64 = 60.0;
const SYSTOLIC_MAX: f64 = 250.0;

const DIASTOLIC_MIN: f64 = 40.0;
const DIASTOLIC_MAX: f64 = 150.0;

const TEMP_MIN: f64 = 35.0;
const TEMP_MAX: f64 = 42.0;

const SPO2_MIN: f64 = 70.0;
const SPO2_MAX: f64 = 100.0;

#[no_mangle]
pub extern "C" fn validate_pulse(value: f64) -> ValidationResult {
    if value >= PULSE_MIN && value <= PULSE_MAX {
        ValidationResult {
            is_valid: true,
            error_code: 0,
            error_message: std::ptr::null_mut(),
        }
    } else {
        let msg = format!(
            "Pulse {} is outside valid range [{}, {}]",
            value, PULSE_MIN, PULSE_MAX
        );
        ValidationResult {
            is_valid: false,
            error_code: 1,
            error_message: CString::new(msg).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn validate_systolic(value: f64) -> ValidationResult {
    if value >= SYSTOLIC_MIN && value <= SYSTOLIC_MAX {
        ValidationResult {
            is_valid: true,
            error_code: 0,
            error_message: std::ptr::null_mut(),
        }
    } else {
        let msg = format!(
            "Systolic pressure {} is outside valid range [{}, {}]",
            value, SYSTOLIC_MIN, SYSTOLIC_MAX
        );
        ValidationResult {
            is_valid: false,
            error_code: 2,
            error_message: CString::new(msg).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn validate_diastolic(value: f64) -> ValidationResult {
    if value >= DIASTOLIC_MIN && value <= DIASTOLIC_MAX {
        ValidationResult {
            is_valid: true,
            error_code: 0,
            error_message: std::ptr::null_mut(),
        }
    } else {
        let msg = format!(
            "Diastolic pressure {} is outside valid range [{}, {}]",
            value, DIASTOLIC_MIN, DIASTOLIC_MAX
        );
        ValidationResult {
            is_valid: false,
            error_code: 3,
            error_message: CString::new(msg).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn validate_temperature(value: f64) -> ValidationResult {
    if value >= TEMP_MIN && value <= TEMP_MAX {
        ValidationResult {
            is_valid: true,
            error_code: 0,
            error_message: std::ptr::null_mut(),
        }
    } else {
        let msg = format!(
            "Temperature {} is outside valid range [{}, {}]",
            value, TEMP_MIN, TEMP_MAX
        );
        ValidationResult {
            is_valid: false,
            error_code: 4,
            error_message: CString::new(msg).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn validate_spo2(value: f64) -> ValidationResult {
    if value >= SPO2_MIN && value <= SPO2_MAX {
        ValidationResult {
            is_valid: true,
            error_code: 0,
            error_message: std::ptr::null_mut(),
        }
    } else {
        let msg = format!(
            "SpO2 {} is outside valid range [{}, {}]",
            value, SPO2_MIN, SPO2_MAX
        );
        ValidationResult {
            is_valid: false,
            error_code: 5,
            error_message: CString::new(msg).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub extern "C" fn free_validation_result(result: ValidationResult) {
    if !result.error_message.is_null() {
        unsafe {
            CString::from_raw(result.error_message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_pulse() {
        let result = validate_pulse(75.0);
        assert!(result.is_valid);
        assert_eq!(result.error_code, 0);
    }

    #[test]
    fn test_invalid_pulse() {
        let result = validate_pulse(250.0);
        assert!(!result.is_valid);
        assert_eq!(result.error_code, 1);
    }

    #[test]
    fn test_valid_temperature() {
        let result = validate_temperature(36.6);
        assert!(result.is_valid);
    }

    #[test]
    fn test_invalid_temperature() {
        let result = validate_temperature(43.0);
        assert!(!result.is_valid);
    }
}
