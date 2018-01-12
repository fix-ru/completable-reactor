package ru.fix.completable.reactor.runtime.gl

internal class GlMergerBuilderImpl<Payload, HandlerResult>(private val vertex: Vertex) : GlMergerBuilder<Payload, HandlerResult> {

    override fun withMerger(merger: Merger<Payload, HandlerResult>): Vertex {
        if (vertex.merger != null) {
            throw IllegalStateException("withMerger method used twice on same vertex")
        }
        vertex.merger = merger
        return vertex
    }

    override fun withMerger(title: String, merger: Merger<Payload, HandlerResult>): Vertex {
        return withMerger(merger)
    }

    override fun withMerger(title: String, doc: String, merger: Merger<Payload, HandlerResult>): Vertex {
        return withMerger(merger)
    }

    override fun withMerger(title: String, docs: Array<String>, merger: Merger<Payload, HandlerResult>): Vertex {
        return withMerger(merger)
    }

    override fun withoutMerger(): Vertex {
        return vertex
    }
}
