package ru.fix.completable.reactor.perf.test

import ru.fix.commons.profiler.Profiler
import ru.fix.completable.reactor.graph.kotlin.Graph
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

class PerfGraph(val profiler: Profiler) : Graph<PerfPayload>() {

    companion object {
        val POOL_SIZE = 100
        val TIMEOUT = 20
    }


    private fun namedPool(name: String, size: Int): ExecutorService {
        val counter = AtomicInteger()

        return Executors.newFixedThreadPool(size) {
            Thread(it, "$name-${counter.getAndIncrement()}")
        }.also {
            (it as ThreadPoolExecutor).let {
                profiler.attachIndicator("pending.$name.tasks"){it.taskCount - it.completedTaskCount}
                profiler.attachIndicator("pending.$name.queue"){it.queue.size.toLong()}
            }
        }
    }




    private val poolHttp = namedPool("pool-http",POOL_SIZE)
    private val poolPG = namedPool("pool-pg",POOL_SIZE)
    private val poolHBase = namedPool("pool-hbase",POOL_SIZE)
    private val poolSmpp = namedPool("pool-smpp",POOL_SIZE)




    private val hp_xmnpmlProcessor = AsyncService(poolHttp, TIMEOUT)

    private val hb_userProfileProcessor = AsyncService(poolHBase, TIMEOUT)

    private val pg_serviceInfoProcessor = AsyncService(poolPG, TIMEOUT)

    private val pg_SmppParametersFetch = AsyncService(poolPG, TIMEOUT)

    private val hb_restrictionProcessor = AsyncService(poolHBase, TIMEOUT)

    private val pg_systemAttributeProcessor = AsyncService(poolPG, TIMEOUT)

    private val hb_contentCountProcessor = AsyncService(poolHBase, TIMEOUT)

    private val sp_sendSmsProcessor = AsyncService(poolSmpp, TIMEOUT)

    private val hb_smsHistoryAndStatisticsProcessor = AsyncService(poolHBase, TIMEOUT)

    private val hb_hBaseClient = AsyncService(poolHBase, TIMEOUT)


    private val hb_readMsisdnInfo =
            handler {
                hb_hBaseClient.asyncMethod(request.arg)

            }.withRoutingMerger { result ->
                if (result.isNotEmpty()) {
                    MsisdnInfoReadResult.NOT_FOUND
                } else {
                    MsisdnInfoReadResult.FOUND
                }
            }

    private val checkTransaction = router {

        if (request.arg.isNullOrBlank()) {
            TxCheckResult.EMPTY
        } else {
            if ((request.arg + 123).length > 3) {
                TxCheckResult.ACTIVE
            } else {
                TxCheckResult.NOT_ACTIVE
            }
        }
    }

    private val readSmppTransactId = handler {

        if (request.arg.isNotEmpty()) {
            CompletableFuture.completedFuture(request.arg)
        } else {
            CompletableFuture.completedFuture("")
        }
    }.withMerger { smppTransactId ->
        intermedium.smppTransactId = smppTransactId
    }

    private val hb_createMsisdnInfoFromXmnpml = handler {
        hp_xmnpmlProcessor.asyncMethod(request.arg)
    }.withRoutingMerger { result ->
        if (result.isEmpty()) {
            CreateMsisdnFromXmnpmlResult.NOT_CREATED
        } else {
            CreateMsisdnFromXmnpmlResult.CREATED
        }
    }

    private val hb_createMsisdnInfoFromOpNumRange = handler {
        hb_userProfileProcessor.asyncMethod(request.arg)
    }.withRoutingMerger { msisdnInfoResult ->
        if (msisdnInfoResult.length > 2) {
            CreateMsisdnFromOperationNumberResult.CREATED
        } else {
            CreateMsisdnFromOperationNumberResult.NOT_CREATED
        }
    }

    private val hb_storeMsisdnInfo = handler {
        hb_hBaseClient.asyncMethod(request.arg)
    }.withEmptyMerger()

    private val pg_readServiceInfo = handler {
        pg_serviceInfoProcessor.asyncMethod(request.arg.length.toString())
    }.withMerger { result -> intermedium.serviceInfo = result }

    private val pg_findServiceInfoByTransaction = handler {
        pg_serviceInfoProcessor.asyncMethod(request.arg)
    }.withRoutingMerger { result ->
        if (result.isNotEmpty()) {
            intermedium.serviceInfo = result
            YesNoResult.YES
        } else {
            YesNoResult.NO
        }
    }

    private val pg_fetchSmppParametersForService = handler {
        pg_SmppParametersFetch.asyncMethod((request.arg.length / 3).toString())
    }.withMerger { result -> intermedium.smppParameters = result }


    private val checkUnlimPush = router {
        if (intermedium.smppParameters.isNotEmpty())
            YesNoResult.YES
        else
            YesNoResult.NO
    }

    private val generateSmppTransactId = router {
        intermedium.smppTransactId = ThreadLocalRandom.current().nextInt().toString()
        YesNoResult.YES
    }

    private val checkServiceIsActive = router {
        val serviceInfo = intermedium.serviceInfo

        if (serviceInfo.isNotEmpty())
            YesNoResult.YES
        else
            YesNoResult.NO
    }


    private val sp_sendSMSContent =

            handler {
                sp_sendSmsProcessor.asyncMethod(
                        intermedium.serviceInfo + intermedium.smppParameters + intermedium.smppTransactId)

            }.withRoutingMerger { response ->
                if (response.isNotEmpty()) {
                    intermedium.smsMsisdnHistory = response + "history"
                    Result.SUCCESS
                } else {
                    Result.FAIL
                }
            }

    private val hb_writeMsisdnTx = handler {
        hb_hBaseClient.asyncMethod(intermedium.serviceInfo + intermedium.smppTransactId)
    }.withEmptyMerger()

    private val hb_checkRestriction = handler {
        hb_restrictionProcessor.asyncMethod(
                intermedium.serviceInfo)
    }.withRoutingMerger { restrictionCheckResult ->
        if (restrictionCheckResult.isEmpty()) {
            RestrictionCheckResult.HAS_RESTRICTION
        } else {
            RestrictionCheckResult.NO_RESTRICTION
        }
    }

    private val hb_incrementAndCheckContentCount = handler {
        hb_contentCountProcessor.asyncMethod(intermedium.smppParameters + intermedium.smppTransactId)
    }.withRoutingMerger {
        if((it + it.length).length > 2) {

            ContentCountProcessorResult.NOT_EXCEEDS
        }
        else {
            ContentCountProcessorResult.EXCEEDS
        }
    }

    private val pg_readMaxPushTimeoutSystemAttribute = handler {
        pg_systemAttributeProcessor.asyncMethod("123")
    }.withMerger { restrictionCheckResult ->
        intermedium.maxPushTimeOut = restrictionCheckResult.trim()
    }


    private val hb_registerInvalidDestinationAddress11 =
            handler {
                logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.INVALID_DEST_ADDRESS)
            }
                    .withMerger { _ -> response.result = VaspSmppConstants.INVALID_DEST_ADDRESS.toString() }


    private val hb_registerInvalidTransaction1315 =
            handler {
                logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.INVALID_TRANSACTION)
            }
                    .withMerger { _ -> response.result = VaspSmppConstants.INVALID_TRANSACTION.toString() }


    private val hb_registerMaxPushTimeOut1376 = handler { logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.MAX_PUSH_TIMEOUT) }.withMerger { _ -> response.result = VaspSmppConstants.MAX_PUSH_TIMEOUT.toString() }
    private val hb_registerServiceNotActive1316 = handler { logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.SERVICE_IS_NOT_ACTIVE) }.withMerger { _ -> response.result = VaspSmppConstants.SERVICE_IS_NOT_ACTIVE.toString() }
    private val hb_registerAbonentHasRestriction1580 = handler { logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.ABONENT_HAS_RESTRICTION) }.withMerger { _ -> response.result = VaspSmppConstants.ABONENT_HAS_RESTRICTION.toString() }
    private val hb_registerCannotDeliver1560 = handler { logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.CANNOT_DELIVER) }.withMerger { _ -> response.result = VaspSmppConstants.CANNOT_DELIVER.toString() }
    private val hb_registerMaxContentReplyExceeded1368 = handler { logSmsFireStatisticsAndSetErrorCode(this, VaspSmppConstants.MAX_REPLY) }.withMerger{ _ -> response.result = VaspSmppConstants.MAX_REPLY.toString() }

    init {

        payload()
                .handleBy(hb_readMsisdnInfo)

        hb_readMsisdnInfo
                .on(MsisdnInfoReadResult.FOUND).handleBy(pg_findServiceInfoByTransaction)
                .on(MsisdnInfoReadResult.NOT_FOUND).handleBy(hb_createMsisdnInfoFromXmnpml)

        pg_findServiceInfoByTransaction
                .on(YesNoResult.YES).handleBy(pg_fetchSmppParametersForService)
                .on(YesNoResult.NO).handleBy(pg_readServiceInfo)

        hb_createMsisdnInfoFromXmnpml
                .on(CreateMsisdnFromXmnpmlResult.CREATED).handleBy(hb_storeMsisdnInfo)
                .on(CreateMsisdnFromXmnpmlResult.NOT_CREATED).handleBy(hb_createMsisdnInfoFromOpNumRange)

        hb_createMsisdnInfoFromOpNumRange
                .on(CreateMsisdnFromOperationNumberResult.CREATED).handleBy(hb_storeMsisdnInfo)
                .on(CreateMsisdnFromOperationNumberResult.NOT_CREATED).handleBy(hb_registerInvalidDestinationAddress11)

        hb_storeMsisdnInfo
                .onAny().handleBy(pg_readServiceInfo);

        pg_readServiceInfo
                .onAny().handleBy(pg_fetchSmppParametersForService);

        pg_fetchSmppParametersForService
                .onAny().handleBy(pg_readMaxPushTimeoutSystemAttribute);

        pg_readMaxPushTimeoutSystemAttribute
                .onAny().handleBy(readSmppTransactId);

        readSmppTransactId
                .onAny().handleBy(checkTransaction);

        checkTransaction
                .on(TxCheckResult.ACTIVE).handleBy(hb_incrementAndCheckContentCount)
                .on(TxCheckResult.NOT_ACTIVE).handleBy(hb_registerMaxPushTimeOut1376)
                .on(TxCheckResult.EMPTY).handleBy(checkUnlimPush);

        checkUnlimPush
                .on(YesNoResult.YES).handleBy(generateSmppTransactId)
                .on(YesNoResult.NO).handleBy(hb_registerInvalidTransaction1315);

        generateSmppTransactId
                .onAny().handleBy(checkServiceIsActive);

        checkServiceIsActive
                .on(YesNoResult.YES).handleBy(hb_checkRestriction)
                .on(YesNoResult.NO).handleBy(hb_registerServiceNotActive1316);

        hb_checkRestriction
                .on(RestrictionCheckResult.HAS_RESTRICTION).handleBy(hb_registerAbonentHasRestriction1580)
                .on(RestrictionCheckResult.BLACK_LISTED).handleBy(hb_registerAbonentHasRestriction1580)
                .on(RestrictionCheckResult.NO_RESTRICTION).handleBy(sp_sendSMSContent);

        hb_incrementAndCheckContentCount
                .on(ContentCountProcessorResult.EXCEEDS).handleBy(hb_registerMaxContentReplyExceeded1368)
                .on(ContentCountProcessorResult.NOT_EXCEEDS).handleBy(sp_sendSMSContent);

        sp_sendSMSContent
                .on(Result.SUCCESS).handleBy(hb_writeMsisdnTx)
                .on(Result.FAIL).handleBy(hb_registerCannotDeliver1560)

        hb_writeMsisdnTx
                .onAny().complete();

        hb_registerInvalidDestinationAddress11.onAny().complete();
        hb_registerInvalidTransaction1315.onAny().complete();
        hb_registerServiceNotActive1316.onAny().complete();
        hb_registerMaxPushTimeOut1376.onAny().complete();
        hb_registerAbonentHasRestriction1580.onAny().complete();
        hb_registerCannotDeliver1560.onAny().complete();
        hb_registerMaxContentReplyExceeded1368.onAny().complete();

        coordinates()
                .pd(-29, -435)
                .vx(checkServiceIsActive, 395, 1018)
                .vx(checkTransaction, 73, 686)
                .vx(checkUnlimPush, 282, 778)
                .vx(generateSmppTransactId, 391, 893)
                .vx(hb_checkRestriction, 283, 1143, 505, 1221)
                .vx(hb_createMsisdnInfoFromOpNumRange, -729, -157, -521, -42)
                .vx(hb_createMsisdnInfoFromXmnpml, -229, -236, -80, -149)
                .vx(pg_fetchSmppParametersForService, -68, 264, 217, 331)
                .vx(pg_findServiceInfoByTransaction, 74, -99, 246, 25)
                .vx(hb_incrementAndCheckContentCount, -179, 908, 56, 1077)
                .vx(pg_readMaxPushTimeoutSystemAttribute, 73, 404, 214, 456)
                .vx(hb_readMsisdnInfo, 10, -383, 67, -314)
                .vx(pg_readServiceInfo, -118, 135, -12, 210)
                .vx(readSmppTransactId, 59, 533, 206, 604)
                .vx(hb_registerAbonentHasRestriction1580, 709, 1348, 845, 1458)
                .vx(hb_registerCannotDeliver1560, 266, 1511, 369, 1599)
                .vx(hb_registerInvalidDestinationAddress11, -669, 105, -552, 266)
                .vx(hb_registerInvalidTransaction1315, 831, 772, 1126, 779)
                .vx(hb_registerMaxContentReplyExceeded1368, -530, 1067, -578, 1079)
                .vx(hb_registerMaxPushTimeOut1376, -343, 739, -545, 754)
                .vx(hb_registerServiceNotActive1316, 787, 1016, 1145, 1019)
                .vx(sp_sendSMSContent, 11, 1343, 106, 1442)
                .vx(hb_storeMsisdnInfo, -87, -31, -10, 62)
                .vx(hb_writeMsisdnTx, -331, 1528, -155, 1602)
                .ct(hb_registerAbonentHasRestriction1580, 845, 1551)
                .ct(hb_registerCannotDeliver1560, 384, 1697)
                .ct(hb_registerInvalidDestinationAddress11, -566, 387)
                .ct(hb_registerInvalidTransaction1315, 1242, 780)
                .ct(hb_registerMaxContentReplyExceeded1368, -752, 1084)
                .ct(hb_registerMaxPushTimeOut1376, -726, 762)
                .ct(hb_registerServiceNotActive1316, 1310, 1018)
                .ct(hb_writeMsisdnTx, -158, 1669);
    }

    internal fun logSmsFireStatisticsAndSetErrorCode(payload: PerfPayload,
                                                     errorCode: VaspSmppConstants): CompletableFuture<String> {

        return hb_smsHistoryAndStatisticsProcessor.asyncMethod(payload.request.arg + payload.intermedium.smppTransactId)
    }

    enum class Result { SUCCESS, FAIL }
    enum class MsisdnInfoReadResult { FOUND, NOT_FOUND }

    enum class TxCheckResult {
        ACTIVE,
        NOT_ACTIVE,
        EMPTY
    }

    enum class SuccessResult {
        SUCCESS
    }

    enum class YesNoResult {
        YES, NO
    }

    enum class RestrictionCheckResult {
        HAS_RESTRICTION,
        BLACK_LISTED,
        NO_RESTRICTION
    }

    enum class CreateMsisdnFromXmnpmlResult {
        CREATED,
        NOT_CREATED
    }

    enum class CreateMsisdnFromOperationNumberResult {
        CREATED,
        NOT_CREATED
    }

    enum class ContentCountProcessorResult {
        EXCEEDS,
        NOT_EXCEEDS
    }

    enum class VaspSmppConstants(val value: Int) {

        INVALID_DEST_ADDRESS(11),

        MAX_PUSH_TIMEOUT(1376),

        INVALID_TRANSACTION(1315),

        SERVICE_IS_NOT_ACTIVE(1316),

        ABONENT_HAS_RESTRICTION(1580),

        CANNOT_DELIVER(1560),

        MAX_REPLY(1368);
    }

}