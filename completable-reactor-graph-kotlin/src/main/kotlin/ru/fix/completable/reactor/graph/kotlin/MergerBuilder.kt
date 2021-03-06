package ru.fix.completable.reactor.graph.kotlin

import ru.fix.completable.reactor.graph.Vertex

interface MergerBuilder<Payload, HandlerResult> {

    //TODO merger that could return any object

    fun withRoutingMerger(merger: Payload.(handlerResult: HandlerResult) -> Enum<*>): Vertex

    fun withMerger(merger: Payload.(handlerResult: HandlerResult) -> Unit): Vertex

    fun withoutMerger(): Vertex
}