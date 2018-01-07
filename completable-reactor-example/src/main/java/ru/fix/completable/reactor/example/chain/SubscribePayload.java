package ru.fix.completable.reactor.example.chain;

import lombok.Data;
import lombok.experimental.Accessors;
import ru.fix.completable.reactor.example.processors.ServiceInfo;
import ru.fix.completable.reactor.example.processors.UserProfile;
import ru.fix.completable.reactor.example.processors.AccountInfo;

/**
 * Created by swarmshine on 16.10.2016.
 */
public class SubscribePayload {

    @Data
    @Accessors()
    public static class Request {
        Long userId;
        Long serviceId;
    }

    @Data
    @Accessors()
    public static class Response {
        Enum status;
        Integer moneyWithdrawed;
    }

    @Data
    @Accessors()
    public static class IntermediateData{
        ServiceInfo serviceInfo;
        AccountInfo accountInfo;
        UserProfile userInfo;
    }

    public final PurchasePayload.Request request = new PurchasePayload.Request();
    public final PurchasePayload.IntermediateData intermediateData = new PurchasePayload.IntermediateData();
    public final PurchasePayload.Response response = new PurchasePayload.Response();

}
