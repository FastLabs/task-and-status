package orchestration.rules

data class ValidationResult(val status: ValidationStatus)

//Almost impossible to have a lock at the event event
//Re-run for the event

enum class ValidationStatus {
    VALID,
    INVALID
}
