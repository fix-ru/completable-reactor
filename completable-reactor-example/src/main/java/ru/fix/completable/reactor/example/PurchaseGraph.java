package ru.fix.completable.reactor.example;

import org.springframework.beans.factory.annotation.Autowired;
import ru.fix.completable.reactor.example.services.*;
import ru.fix.completable.reactor.graph.Graph;
import ru.fix.completable.reactor.graph.Vertex;

import java.math.BigDecimal;

/**
 * Define purchase process when user buys good in the shop.
 */
public class PurchaseGraph extends Graph<PurchasePayload> {

    Vertex userProfile = new Vertex() {
        UserProfileManager userProfile;

        {
            handler(
                    //# load user profile
                    pld -> userProfile.loadUserProfileById(pld.request.getUserId())
            ).withMerger(
                    //# check profile state
                    (pld, result) -> {
                        if (pld.response.getStatus() != null) {
                            return Flow.STOP;
                        }

                        switch (result.status) {
                            case USER_NOT_FOUND:
                            case USER_IS_BLOCKED:
                                pld.response.setStatus(result.status);
                                return Flow.STOP;

                            case OK:
                                pld.intermediateData.setUserInfo(result.userProfile);
                                return Flow.CONTINUE;
                        }
                        throw new IllegalArgumentException("result.status = " + result.status);
                    });
        }
    };

    Vertex txLog = new Vertex() {
        TransactionLog txLog;

        {
            handler(
                    pld -> txLog.logTransactioin(pld.request.getUserId())
            ).withMerger(
                    (pld, any) -> Flow.CONTINUE
            );
        }
    };

    Vertex userJournal = new Vertex() {
        UserJournal userJournal;

        {
            handler(
                    pld -> userJournal.logAction(
                            pld.request.getUserId(),
                            String.format("Request type: %s", pld.getClass().getSimpleName()))
            ).withMerger((pld, result) -> Flow.CONTINUE);
        }
    };

    @Autowired
    Notifier notifier;


    Vertex webNotification =
            handler(pld -> notifier.sendHttpPurchaseNotification(pld.request.getUserId()))
                    .withoutMerger();

    Vertex smsNotification =
            handler(pld -> notifier.sendSmsPurchaseNotification(pld.request.getUserId()))
                    .withoutMerger();


    Vertex bank = new Vertex() {
        Bank bank;

        {
            handler(/*
                        # WithdrawMoneyMinus
                        Withdraw money even if user does not have money on his account.
                        User will end up with negative balance after this operation.
                    */
                    pld -> bank.withdrawMoneyWithMinus(
                            pld.intermediateData.getUserInfo(),
                            pld.intermediateData.getServiceInfo())
            ).withMerger(
                    /*
                        # CheckWithdraw
                        Checks result of withdraw operation
                        Sets new amount and withdrawal status in payload
                        Stops in case if operation is failed
                    */
                    (pld, withdraw) -> {
                        switch (withdraw.getStatus()) {
                            case WALLET_NOT_FOUND:
                            case USER_IS_BLOCKED:
                                pld.response.setStatus(withdraw.getStatus());
                                return Flow.STOP;
                            case OK:
                                pld.response
                                        .setNewAmount(withdraw.getNewAmount())
                                        .setWithdrawalWasInMinus(withdraw.getNewAmount().compareTo(BigDecimal.ZERO) < 0)
                                        .setStatus(Bank.Withdraw.Status.OK);
                                return Flow.CONTINUE;
                            default:
                                throw new IllegalArgumentException("Status: " + withdraw.getStatus());
                        }
                    });
        }
    };

    Vertex isPartnerService =
            router(/*
                       Check if given service is provided by a partner.
                       Update payload response.
                   */
                    pld -> {
                        if (pld.intermediateData.serviceInfo.isPartnerService()) {
                            pld.response.isPartnerService = true;
                            return Flow.CONTINUE;
                        } else {
                            return Flow.CONTINUE;
                        }
                    });

    Vertex serviceInfo = new Vertex() {
        ServiceRegistry serviceRegistry;

        {
            handler(pld -> serviceRegistry.loadServiceInformation(pld.request.getServiceId()))
                    .withMerger(
                            //# checkServiceState
                            (pld, result) -> {
                                if (pld.response.getStatus() != null) {
                                    return Flow.STOP;
                                }

                                switch (result.getStatus()) {
                                    case SERVICE_NOT_FOUND:
                                        pld.response.setStatus(result.getStatus());
                                        return Flow.STOP;
                                    case OK:
                                        pld.intermediateData.setServiceInfo(result.getServiceInfo());

                                        if (result.getServiceInfo().isActive()) {
                                            return Flow.WITHDRAWAL;
                                        } else {
                                            return Flow.NO_WITHDRAWAL;
                                        }
                                }
                                return Flow.CONTINUE;
                            });
        }
    };

    Vertex marketingCampaign = new Vertex() {
        MarketingService marketingService;

        {
            handlerSync(pld -> marketingService.checkBonuses(pld.request.userId, pld.request.serviceId)
            ).withMerger((pld, bonus) -> {
                if (bonus.isPresent()) {
                    pld.intermediateData.bonusService = bonus.get();
                    return Flow.BONUS_EXIST;
                } else {
                    return Flow.NO_BONUS;
                }
            });
        }
    };

    Vertex bonusPurchaseSubgraph =
            subgraph(
                    PurchasePayload.class,
                    pld -> {
                        PurchasePayload subgraphRequest = new PurchasePayload();
                        subgraphRequest.request
                                .setServiceId(107L)
                                .setUserId(pld.request.userId);
                        return subgraphRequest;
                    }

            ).withMerger((pld, subgraphResult) ->
                    pld.response.bonusServiceStatus = subgraphResult.response.status
            );

    {

        payload()
                .handleBy(userProfile)
                .handleBy(serviceInfo);

        userProfile
                .on(Flow.STOP).complete()
                .on(Flow.CONTINUE).mergeBy(serviceInfo);

        serviceInfo
                .on(Flow.WITHDRAWAL).handleBy(bank)
                .on(Flow.NO_WITHDRAWAL).handleBy(userJournal)
                .on(Flow.NO_WITHDRAWAL).handleBy(webNotification)
                .on(Flow.NO_WITHDRAWAL).handleBy(smsNotification)
                .on(Flow.STOP).complete();

        bank.onAny().handleBy(isPartnerService);

        isPartnerService.onAny().handleBy(txLog);

        txLog.onAny().handleBy(userJournal);

        userJournal.onAny().handleBy(marketingCampaign);

        marketingCampaign
                .on(Flow.BONUS_EXIST).handleBy(bonusPurchaseSubgraph)
                .on(Flow.NO_BONUS).complete();

        bonusPurchaseSubgraph.onAny().complete();

        coordinates()
                .payload(680, 60)
                .handler(bank, 410, 440)
                .handler(webNotification, 880, 430)
                .handler(smsNotification, 850, 450)
                .handler(serviceInfo, 480, 120)
                .handler(txLog, 420, 650)
                .handler(userJournal, 680, 820)
                .handler(userProfile, 770, 120)
                .merger(bank, 480, 550)
                .merger(serviceInfo, 640, 280)
                .merger(txLog, 530, 770)
                .merger(userJournal, 760, 930)
                .merger(userProfile, 806, 201)
                .complete(serviceInfo, 480, 310)
                .complete(userProfile, 963, 258);
    }
}