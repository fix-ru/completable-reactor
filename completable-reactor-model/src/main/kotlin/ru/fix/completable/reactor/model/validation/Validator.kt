package ru.fix.completable.reactor.model.validation

import ru.fix.completable.reactor.model.CompileTimeGraphModel


sealed class ValidationResult

class ValidationSucceed() : ValidationResult()

class ValidationFailed(val message: String) : ValidationResult()

interface Validator {

    fun validate(graph: CompileTimeGraphModel): ValidationResult
}