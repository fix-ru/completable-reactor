package ru.fix.completable.reactor.runtime.execution

import mu.KotlinLogging
import ru.fix.completable.reactor.graph.runtime.RuntimeVertex
import ru.fix.completable.reactor.runtime.ProfilerIdentity
import ru.fix.completable.reactor.runtime.execution.ExecutionBuilder.Companion.INVALID_TRANSITION_PAYLOAD_CONTEXT
import ru.fix.completable.reactor.runtime.execution.ExecutionBuilder.HandlePayloadContext
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

class HandleByExecutionBuilder<PayloadType>(
        val processingVertices: IdentityHashMap<RuntimeVertex, ProcessingVertex>,
        val executionResultFuture: CompletableFuture<PayloadType>,
        val builder: ExecutionBuilder) {

    /**
     * Join incoming handling flows to single handling invocation
     * All incoming handling transitions should complete.
     * One of them should carry payload.
     * Other transitions should complete with isDead flag.
     */
    fun joinIncomingHandleByFlowsToSingleHandlingInvocation() {
        //TODO FIX!! allow detached processor to read data from payload and only after that execute merge point
        // down the flow, so merge point and detached processor would not run concurrently


        for ((vx, pvx) in processingVertices) {

            if (pvx.incomingHandlingFlows.size <= 0) {
                throw IllegalArgumentException("""
                        Invalid graph configuration.
                        Vertex ${vx.name} does not have incoming handling flows.
                        Probably missing `.handleBy(${vx.name})` transition that targets this vertex in configuration.
                        """.trimIndent())
            }

            /**
             * First we should wait for all incoming handleBy transition to complete
             */
            CompletableFuture.allOf(
                    *pvx.incomingHandlingFlows.asSequence().map { it.feature }.toList().toTypedArray()

            ).thenRunAsync {

                val incomingFlows: List<ExecutionBuilder.TransitionPayloadContext> = pvx
                        .incomingHandlingFlows
                        .stream()
                        .map { future ->
                            try {
                                /**
                                 * Future should be already complete.
                                 */
                                if (!future.feature.isDone()) {
                                    val resultException = Exception("""
                                        Illegal graph execution state.
                                        Future is not completed. Vertex: ${vx.name}
                                        """.trimIndent())

                                    log.error(resultException) {}
                                    executionResultFuture.completeExceptionally(resultException)

                                    INVALID_TRANSITION_PAYLOAD_CONTEXT
                                } else {
                                    future.feature.get()
                                }
                            } catch (exc: Exception) {
                                val resultException = Exception("""
                                        Failed to get incoming processor flow future result
                                        for vertex: ${vx.name}
                                        """.trimIndent())
                                log.error(resultException) {}
                                executionResultFuture.completeExceptionally(resultException)

                                INVALID_TRANSITION_PAYLOAD_CONTEXT
                            }
                        }
                        .collect(Collectors.toList())

                if (incomingFlows.stream().anyMatch { context -> context === INVALID_TRANSITION_PAYLOAD_CONTEXT }) {
                    /**
                     * Invalid graph execution state
                     * Mark as terminal all outgoing flows from vertex
                     */
                    pvx.handlingFuture.complete(ExecutionBuilder.HandlePayloadContext(isTerminal = true))

                } else if (incomingFlows.stream().anyMatch(ExecutionBuilder.TransitionPayloadContext::isTerminal)) {
                    /**
                     * Terminal state reached.
                     * Mark as terminal all outgoing flows from vertex
                     */
                    pvx.handlingFuture.complete(ExecutionBuilder.HandlePayloadContext(isTerminal = true))

                } else {
                    val activeIncomingFlows: List<ExecutionBuilder.TransitionPayloadContext> = incomingFlows
                            .stream()
                            .filter { context -> !context.isDeadTransition }
                            .collect(Collectors.toList())

                    if (activeIncomingFlows.isEmpty()) {
                        /**
                         * There is no active incoming flow for given vertex.
                         * Vertex will not be invoked.
                         * All outgoing flows from vertex will be marked as dead.
                         */
                        pvx.handlingFuture.complete(ExecutionBuilder.HandlePayloadContext(isDeadTransition = true))

                    } else {

                        /**
                         * If there is a single active incoming flow - use transition payload context from this flow.
                         * If there are many active incoming flows - select any of them:
                         * we are using single payload instance for whole graph execution, no matter from which of
                         * active transition we will select it.
                         */
                        handle(pvx, activeIncomingFlows[0], executionResultFuture)
                    }
                }
            }
                    .exceptionally { throwable ->
                        log.error("Join incoming handleBy flows failed for vertex ${vx.name}.", throwable)
                        null
                    }

        }//end of handleBy flows
    }


    private fun <PayloadType> handle(
            pvx: ProcessingVertex,
            payloadContext: ExecutionBuilder.TransitionPayloadContext,
            executionResultFuture: CompletableFuture<PayloadType>) {

        val vx = pvx.vertex
        val payload = payloadContext.payload

        val handleCall = builder.profiler
                .profiledCall(ProfilerIdentity.handleIdentity(payload?.javaClass?.name, vx.name))
                .start()

        val isTraceablePayload = builder.tracer.isTraceable(payload)
        val handleTracingMarker =
                if (isTraceablePayload) {
                    builder.tracer.beforeHandle(vx.name, payload)
                } else {
                    null
                }
        val handleTracingIdentity =
                if (isTraceablePayload) {
                    vx.name
                } else {
                    null
                }
        val handlingResult: CompletableFuture<Any?>

        try {
            handlingResult = pvx.invokeHandlingMethod(payload)

        } catch (handlingException: Exception) {
            val exc = RuntimeException(
                    """
                    Failed to run handling method by veretx ${vx.name} for payload ${builder.debugSerializer.dumpObject(payload)}.
                    Handling method raised an exception: $handlingException.
                    """.trimIndent(),
                    handlingException)

            log.error(exc) {}
            executionResultFuture.completeExceptionally(exc)
            pvx.handlingFuture.complete(ExecutionBuilder.HandlePayloadContext(isTerminal = true))
            handleCall.stop()
            return
        }

        handlingResult.handleAsync { result, resultThrowable ->

            var throwable = resultThrowable

            handleCall.stop()

            if (isTraceablePayload) {
                builder.tracer.afterHandle(handleTracingMarker, handleTracingIdentity, result, throwable)
            }

            if (throwable != null) {
                val exc = RuntimeException(
                        """
                        Failed handling by vertex ${vx.name} for payload ${builder.debugSerializer.dumpObject(payload)}
                        """.trimIndent(),
                        throwable)

                executionResultFuture.completeExceptionally(exc)

                pvx.handlingFuture.complete(HandlePayloadContext(isTerminal = true))
            } else {
                pvx.handlingFuture.complete(HandlePayloadContext(payload = payload, handlingResult = result))
            }

            null

        }.exceptionally { exc ->
            log.error(exc) { "Failed to execute afterHandle block for vertex ${pvx.vertex.name}" }
            null
        }
    }

}