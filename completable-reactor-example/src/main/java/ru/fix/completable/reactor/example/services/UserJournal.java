package ru.fix.completable.reactor.example.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.completable.reactor.api.Reactored;

import java.util.concurrent.CompletableFuture;


/**
 * @author Kamil Asfandiyarov
 */
@Reactored({
        "Logs information about usres actions.",
        "Log contains user id",
})
public class UserJournal {
    private static final Logger log = LoggerFactory.getLogger(UserJournal.class);

    public CompletableFuture<Void> logAction(Long userId, String message) {
        log.info("Userlog id: {} message: {}", userId, message);
        return CompletableFuture.completedFuture(null);
    }
}
