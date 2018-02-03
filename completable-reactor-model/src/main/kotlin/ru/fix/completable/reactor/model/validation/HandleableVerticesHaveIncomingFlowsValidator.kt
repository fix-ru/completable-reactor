package ru.fix.completable.reactor.model.validation

import ru.fix.completable.reactor.model.GraphModel
import ru.fix.completable.reactor.model.HandleableVertexFigure

class HandleableVerticesHaveIncomingFlowsValidator : Validator {

    override fun validate(graph: GraphModel): ValidationResult {

        val verticesWithIncomingTransitions = graph.transitionable
                //collect all transitions targets
                .values
                .asIterable()
                .flatMap { it.transitions }
                .mapNotNull { it.target as? HandleableVertexFigure }
                //add start point transitions
                .union(graph.startPoint.handleBy)
                .map { it.name }
                .toHashSet()


        val verticesWithoutIncomingFlows = graph.handleable
                .values
                .asIterable()
                .map { it.name }
                .filter { verticesWithIncomingTransitions.contains(it) }
                .toList()

        return if (verticesWithoutIncomingFlows.isNotEmpty()) {
            ValidationFailed("ProcessingItem %s does not have incoming transitions.")
        } else {
            ValidationSucceed()
        }
    }
}