typedef struct {
    bool is_valid;
    int error_code;
    char* error_message;
} ValidationResult;

ValidationResult validate_pulse(double value);
ValidationResult validate_systolic(double value);
ValidationResult validate_diastolic(double value);
ValidationResult validate_temperature(double value);
ValidationResult validate_spo2(double value);
void free_validation_result(ValidationResult result);
