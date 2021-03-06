package ru.fix.completable.reactor.parser.java

import mu.KotlinLogging
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThanOrEqualTo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.fix.completable.reactor.model.Coordinates
import ru.fix.completable.reactor.model.EndPoint
import ru.fix.completable.reactor.model.Merger
import ru.fix.completable.reactor.model.VertexFigure
import ru.fix.completable.reactor.test.utils.ProjectFileResolver
import java.time.Duration
import java.time.Instant


private val log = KotlinLogging.logger {}

/**
 * @author Kamil Asfandiyarov
 */
class JavaSourceParserTest {

    @Test
    fun build_compilation_model_for_single_purchase_graph_in_java() {

        val sourceFilePath = "completable-reactor-example/src/main/java/ru/fix/completable/reactor/example/PurchaseGraph.java"
        val body = ProjectFileResolver().read(sourceFilePath)

        val startTime = Instant.now()

        val models = JavaSourceParser(object : JavaSourceParser.Listener {
            override fun error(msg: String) {
                log.error { msg }
                fail<Nothing>(msg)
            }
        }).parse(body, sourceFilePath)

        log.info { "Parsing took ${Duration.between(startTime, Instant.now()).toMillis()}ms" }

        assertEquals(1, models.size)

        val model = models[0]

        fun vertexTransitions(name: String) = model.transitionable[name]!!.transitions.asSequence()


        val LOAD_USER_PROFILE = "loadUserProfile"
        val TX_LOG = "logTransaction"
        val TX_LOG2 = "logTransaction2"
        val USER_JOURNAL = "logActionToUserJournal"
        val WEB_NOTIFICATION = "sendWebNotification"
        val SMS_NOTIFICATION = "sendSmsNotification"
        val BANK = "withdrawMoneyWithMinus"
        val IS_PARTNER_SERVICE = "isPartnerService"
        val SERVICE_INFO = "loadServiceInfo"
        val MARKETING_CAMPAIGN = "checkBonuses"
        val BONUS_PURCHASE_SUBGRAPH = "bonusPurchaseSubgraph"

        assertTrue(model.handlers.containsKey(LOAD_USER_PROFILE))
        assertTrue(model.handlers.containsKey(TX_LOG))
        assertTrue(model.handlers.containsKey(USER_JOURNAL))
        assertTrue(model.handlers.containsKey(WEB_NOTIFICATION))
        assertTrue(model.handlers.containsKey(SMS_NOTIFICATION))
        assertTrue(model.handlers.containsKey(BANK))
        assertTrue(model.handlers.containsKey(SERVICE_INFO))
        assertTrue(model.routers.containsKey(MARKETING_CAMPAIGN))
        assertTrue(model.subgraphs.containsKey(BONUS_PURCHASE_SUBGRAPH))
        assertTrue(model.routers.containsKey(IS_PARTNER_SERVICE))


        assertEquals("PurchasePayload", model.startPoint.payloadType)
        assertEquals("PurchaseGraph", model.graphClass)
        assertEquals("Defines purchase process when user buys good in the shop.", model.startPoint.title)

        assertEquals(
                listOf(LOAD_USER_PROFILE, SERVICE_INFO),
                model.startPoint.handleBy.map { it.name })

        vertexTransitions(LOAD_USER_PROFILE)
                .apply {
                    assertEquals(2, count())

                    assertNotNull(find {
                        it.mergeStatuses == setOf("STOP")
                                && it.isComplete
                                && !it.isOnAny
                                && it.target is EndPoint
                    })

                    assertEquals(
                            Coordinates(897, 225),
                            find { it.mergeStatuses == setOf("STOP") }?.target?.coordinates)

                    assertNotNull(find {
                        it.mergeStatuses == setOf("CONTINUE")
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let {
                            it is VertexFigure && it.name == SERVICE_INFO
                        }
                    })
                }

        vertexTransitions(SERVICE_INFO)
                .apply {
                    assertEquals(5, count())

                    assertNotNull(find {
                        it.mergeStatuses == setOf("WITHDRAWAL")
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let {
                            it is VertexFigure && it.name == BANK
                        }
                    })

                    assertNotNull(find {
                        it.mergeStatuses == setOf("NO_WITHDRAWAL")
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let {
                            it is VertexFigure && it.name == TX_LOG2
                        }
                    })

                    assertNotNull(find {
                        it.mergeStatuses == setOf("NO_WITHDRAWAL")
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let {
                            it is VertexFigure && it.name == WEB_NOTIFICATION
                        }
                    })

                    assertNotNull(find {
                        it.mergeStatuses == setOf("NO_WITHDRAWAL")
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let {
                            it is VertexFigure && it.name == SMS_NOTIFICATION
                        }
                    })

                    assertNotNull(find {
                        it.mergeStatuses == setOf("STOP")
                                && it.isComplete
                                && !it.isOnAny
                                && it.target is EndPoint
                    })

                    assertEquals(
                            Coordinates(497, 293),
                            find { it.mergeStatuses == setOf("STOP") }?.target?.coordinates)
                }

        vertexTransitions(BANK)
                .apply {
                    assertEquals(1, count())

                    assertNotNull(find {
                        it.mergeStatuses.isEmpty()
                                && !it.isComplete
                                && it.isOnAny
                                && it.target.let { it is VertexFigure && it.name == IS_PARTNER_SERVICE }
                    })
                }

        vertexTransitions(IS_PARTNER_SERVICE)
                .apply {
                    assertEquals(1, count())

                    assertNotNull(find {
                        it.mergeStatuses.isEmpty()
                                && !it.isComplete
                                && it.isOnAny
                                && it.target.let { it is VertexFigure && it.name == TX_LOG }
                    })
                }

        vertexTransitions(TX_LOG)
                .apply {
                    assertEquals(1, count())

                    assertNotNull(find {
                        it.mergeStatuses.isEmpty()
                                && !it.isComplete
                                && it.isOnAny
                                && it.target.let { it is VertexFigure && it.name == USER_JOURNAL }
                    })
                }

        vertexTransitions(TX_LOG2)
                .apply {
                    assertEquals(1, count())

                    assertNotNull(find {
                        it.mergeStatuses.isEmpty()
                                && !it.isComplete
                                && it.isOnAny
                                && it.target.let { it is VertexFigure && it.name == USER_JOURNAL }
                    })
                }

        vertexTransitions(USER_JOURNAL)
                .apply {
                    assertEquals(1, count())

                    assertNotNull(find {
                        it.mergeStatuses.isEmpty()
                                && !it.isComplete
                                && it.isOnAny
                                && it.target.let { it is VertexFigure && it.name == MARKETING_CAMPAIGN }
                    })
                }

        vertexTransitions(MARKETING_CAMPAIGN)
                .apply {
                    assertEquals(2, count())

                    assertNotNull(find {
                        it.mergeStatuses.singleOrNull() == "BONUS_EXIST"
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let { it is VertexFigure && it.name == BONUS_PURCHASE_SUBGRAPH }
                    }?.also {
                        val docs = it.transitionDocs.singleOrNull()
                        assertNotNull(docs)
                        assertEquals("BONUS_EXIST", docs!!.mergeStatus)
                        assertEquals("User can claim bonus purchase", docs.docs)
                    })

                    assertNotNull(find {
                        it.mergeStatuses.singleOrNull() == "NO_BONUS"
                                && it.isComplete
                                && !it.isOnAny
                                && it.target.let { it is EndPoint }
                    }?.also {
                        val docs = it.transitionDocs.singleOrNull()
                        assertNotNull(docs)
                        assertEquals("NO_BONUS", docs!!.mergeStatus)
                        assertEquals("User does not have any bonuses", docs.docs)
                    })
                }

        assertEquals("CheckWithdraw", model.mergers[BANK]!!.title)
        assertEquals("check profile state", model.mergers[LOAD_USER_PROFILE]!!.title)
        assertEquals("load user profile", model.handlers[LOAD_USER_PROFILE]!!.title)
        assertEquals("Make bonus purchase", model.subgraphs[BONUS_PURCHASE_SUBGRAPH]!!.title)

        assertNull(model.routers[IS_PARTNER_SERVICE]!!.title)
        assertTrue {
            model.routers[IS_PARTNER_SERVICE]!!
                    .doc!!
                    .startsWith("Check if given service is provided by a partner.")
        }


        """\.pd\((\d+), (\d+)\)""".toRegex().find(body)!!.let {
            assertEquals(
                    Coordinates(it.groupValues[1].toInt(), it.groupValues[2].toInt()),
                    model.startPoint.coordinates)
        }

        """\.vx\($BANK, (\d+), (\d+), (\d+), (\d+)\)""".toRegex().find(body)!!.let {
            assertEquals(
                    Coordinates(it.groupValues[1].toInt(), it.groupValues[2].toInt()),
                    model.handleable[BANK]!!.coordinates)
            assertEquals(
                    Coordinates(it.groupValues[3].toInt(), it.groupValues[4].toInt()),
                    model.mergers[BANK]!!.coordinates)
        }

        """\.vx\($WEB_NOTIFICATION, (\d+), (\d+)\)""".toRegex().find(body)!!.let {
            assertEquals(
                    Coordinates(it.groupValues[1].toInt(), it.groupValues[2].toInt()),
                    model.handleable[WEB_NOTIFICATION]!!.coordinates)
        }

        """\.ct\($SERVICE_INFO, (\d+), (\d+)\)""".toRegex().find(body)!!.let {
            assertEquals(
                    Coordinates(it.groupValues[1].toInt(), it.groupValues[2].toInt()),
                    model.endpoints[SERVICE_INFO]!!.coordinates)
        }

        model.startPoint.source!!.let {
            assertEquals(sourceFilePath, it.filePath)
            assertTrue(it.line > 0)
            assertTrue(it.offset > 0)
            assertTrue(it.column > 0)
        }

        model.handlers[BANK]!!.source!!.let {
            assertEquals(sourceFilePath, it.filePath)
            assertTrue(it.line > 0)
            assertTrue(it.offset > 0)
            assertTrue(it.column > 0)
        }

    }

    @Test
    fun two_test_graphs_in_single_source() {
        val sourceFilePath = "completable-reactor-runtime/src/test/java/ru/fix/completable/reactor/runtime/tests/CompletableReactorTest.java"
        val body = ProjectFileResolver().read(sourceFilePath)

        val startTime = Instant.now()

        val models = JavaSourceParser(object : JavaSourceParser.Listener {
            override fun error(msg: String) {
                log.error { msg }
                fail<Nothing>(msg)
            }
        }).parse(body, sourceFilePath)

        log.info { "Parsing took ${Duration.between(startTime, Instant.now()).toMillis()}ms" }

        assertThat(models.size, greaterThanOrEqualTo(2))

        val singleProcessorGraph = models
                .find { it.graphClass == "SingleProcessorGraph" }
                ?: fail()

        assertNotNull(singleProcessorGraph.handlers["idProcessor1"])

        val twoProcesssorSequenceGraph = models
                .find { it.graphClass == "TwoProcessorSequentialMergeGraph" }
                ?: fail()

        assertNotNull(twoProcesssorSequenceGraph.handlers["idProcessor1"])
        assertNotNull(twoProcesssorSequenceGraph.handlers["idProcessor2"])

        assertNull(twoProcesssorSequenceGraph.coordinatesStart)
        assertNull(twoProcesssorSequenceGraph.coordinatesEnd)
    }


    //TODO check why kotlin parsed in 240ms and java parssed in 66ms, in same time antlr viewer shows same result ~20ms
    @Test
    fun build_compilation_model_for_single_subscribe_graph_in_kotlin() {
        val sourceFilePath = "completable-reactor-example/src/main/kotlin/ru/fix/completable/reactor/example/SubscribeGraph.kt"
        val body = ProjectFileResolver().read(sourceFilePath)

        val startTime = Instant.now()

        val models = JavaSourceParser(object : JavaSourceParser.Listener {
            override fun error(msg: String) {
                log.error { msg }
                fail<Nothing>(msg)
            }
        }).parse(body, sourceFilePath)

        log.info { "Parsing took ${Duration.between(startTime, Instant.now()).toMillis()}ms" }

        assertEquals(1, models.size)

        val model = models[0]

        fun vertexTransitions(name: String) = model.transitionable[name]!!.transitions.asSequence()


        val LOAD_USER_PROFILE = "loadUserProfile"
        val TX_LOG = "logTransaction"
        val TX_LOG2 = "logTransaction2"
        val USER_JOURNAL = "logActionToUserJournal"


        val BANK = "withdrawMoney"
        val PURCHASE_SUBGRPAPH = "bonusPurchaseSubgraph"
        val SERVICE_INFO = "loadServiceInfo"
        val CHECK_TRIAL_PERIOD = "checkTrialPeriod"

        assertTrue(model.handlers.containsKey(LOAD_USER_PROFILE))
        assertTrue(model.handlers.containsKey(TX_LOG))
        assertTrue(model.handlers.containsKey(TX_LOG2))
        assertTrue(model.handlers.containsKey(USER_JOURNAL))
        assertTrue(model.handlers.containsKey(BANK))
        assertTrue(model.handlers.containsKey(SERVICE_INFO))
        assertTrue(model.subgraphs.containsKey(PURCHASE_SUBGRPAPH))
        assertTrue(model.routers.containsKey(CHECK_TRIAL_PERIOD))


        assertEquals("SubscribePayload", model.startPoint.payloadType)
        assertEquals("SubscribeGraph", model.graphClass)
        assertEquals("Subscribe user to service and regularly withdraw money", model.startPoint.title)
        assertEquals("Make bonus purchase", model.subgraphs[PURCHASE_SUBGRPAPH]!!.title)

        assertEquals(
                listOf(LOAD_USER_PROFILE, SERVICE_INFO),
                model.startPoint.handleBy.map { it.name })

        vertexTransitions(LOAD_USER_PROFILE)
                .apply {
                    assertEquals(2, count())

                    assertNotNull(find {
                        it.mergeStatuses == setOf("STOP")
                                && it.isComplete
                                && !it.isOnAny
                                && it.target is EndPoint
                    })

                    assertEquals(
                            Coordinates(924, 257),
                            find { it.mergeStatuses == setOf("STOP") }?.target?.coordinates)

                    assertNotNull(find {
                        it.mergeStatuses == setOf("CONTINUE")
                                && !it.isComplete
                                && !it.isOnAny
                                && it.target.let {
                            it is VertexFigure && it.name == SERVICE_INFO
                        }
                    })
                }

    }

    @Test
    fun `several kotlin graphs from tests`() {
        val sourceFilePath = "completable-reactor-graph-kotlin/src/test/kotlin/ru/fix/completable/reactor/graph/kotlin/KotlinGraphTest.kt"
        val body = ProjectFileResolver().read(sourceFilePath)

        val startTime = Instant.now()

        val models = JavaSourceParser(object : JavaSourceParser.Listener {
            override fun error(msg: String) {
                log.error { msg }
            }
        }).parse(body, sourceFilePath)

        log.info { "Parsing took ${Duration.between(startTime, Instant.now()).toMillis()}ms" }

        assertThat(models.size, greaterThanOrEqualTo(2))

        val implicitMergerGraph = models.find { it.graphClass == "ImplicitEmptyMergerWithOnAnyTransitionGraph" }
                ?: fail()

        assertNotNull(implicitMergerGraph.handlers["vertex11"])
        assertNotNull(implicitMergerGraph.mergers["vertex11"])
        assertNotNull(implicitMergerGraph.handlers["vertex12"])
        assertNotNull(implicitMergerGraph.mergers["vertex12"])
        assertNotNull(implicitMergerGraph.handlers["vertex2"])
        assertNotNull(implicitMergerGraph.mergers["vertex2"])

        assertEquals(
                setOf("vertex11", "vertex12"),
                implicitMergerGraph.startPoint.handleBy.asSequence().map { it.name }.toSet())

        implicitMergerGraph.mergers["vertex11"]!!.transitions.single().let {
            assertTrue(it.isOnAny)
            assertTrue(it.target is Merger)
            assertEquals ("vertex12", (it.target as Merger).name)
        }


        implicitMergerGraph.mergers["vertex12"]!!.transitions.single().let {
            assertTrue(it.isOnAny)
            assertFalse(it.isComplete)
            assertEquals("vertex2", (it.target as VertexFigure).name)
        }


        implicitMergerGraph.mergers["vertex2"]!!.transitions.single().let {
            assertEquals(true, it.isOnAny)
            assertEquals(true, it.isComplete)
            assertTrue { it.target is EndPoint }
        }


    }
}

