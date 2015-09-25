package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.constants.Addresses;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.TimeoutStream;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static de.codepitbull.vertx.eventsourcing.constants.Addresses.*;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_MISSING_PARAMETER;
import static org.apache.commons.lang3.Validate.notNull;
import static rx.observables.JoinObservable.from;
import static rx.observables.JoinObservable.when;

/**
 * Instances of this verticle handle storage and replay of game events.
 *
 * @author Jochen Mader
 */
public class EventStoreVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreVerticle.class);

    private String gameId;
    private Integer spectatorCounter = 0;

    private List<JsonObject> snapshots = new ArrayList<>();
    private List<JsonObject> updates = new ArrayList<>();
    private Map<Integer, SpectatorData> spectatorIdToData = new HashMap<>();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        gameId = notNull(config().getString(GAME_ID));

        MessageConsumer<JsonObject> snapshotsConsumer = vertx.eventBus().<JsonObject>consumer(REPLAY_SNAPSHOTS_BASE + gameId);
        snapshotsConsumer.handler(this::handleSnapshots);

        MessageConsumer<JsonObject> updatesConsumer = vertx.eventBus().<JsonObject>consumer(REPLAY_UPDATES_BASE + gameId);
        updatesConsumer.handler(this::handleUpdates);

        MessageConsumer<JsonObject> registerConsumer = vertx.eventBus().<JsonObject>consumer(REPLAY_REGISTER_BASE + gameId);
        registerConsumer.handler(this::handleConsumerRegistration);

        MessageConsumer<JsonObject> startConsumer = vertx.eventBus().<JsonObject>consumer(REPLAY_START_BASE + gameId);
        startConsumer.bodyStream().handler(this::handleReplay);

        when(
                from(registerConsumer.completionHandlerObservable())
                        .and(snapshotsConsumer.completionHandlerObservable())
                        .and(updatesConsumer.completionHandlerObservable())
                        .and(startConsumer.completionHandlerObservable())
                        .then((a, b, c, d) -> null)
        ).toObservable().subscribe(
                success -> {
                    LOG.info("Succeeded deploying " + EventStoreVerticle.class);
                    startFuture.complete();
                },
                failure -> {
                    LOG.info("Failed deploying " + EventStoreVerticle.class, failure);
                    startFuture.fail(failure);
                }
        );

    }

    private void handleConsumerRegistration(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        if (body.containsKey(REPLAY_INDEX)) {
            ++spectatorCounter;
            LOG.info("Registered spectator " + spectatorCounter + " for game " + gameId);
            spectatorIdToData.put(spectatorCounter, new SpectatorData(body.getInteger(REPLAY_INDEX)));
            msg.reply(spectatorCounter);
        } else msg.fail(FAILURE_MISSING_PARAMETER.intValue(), "Missing " + REPLAY_INDEX);
    }

    private void handleReplay(JsonObject body) {
        Integer spectatorId = body.getInteger(SPECTATOR_ID);
        LOG.info("Streaming to "+Addresses.BROWSER_SPECTATOR_BASE + gameId + "." + spectatorId);
        SpectatorData data = spectatorIdToData.get(spectatorId);
        if (data.timeoutStream != null) data.timeoutStream.cancel();
        JsonObject startSnapshot = snapshots.get(data.startIndex);
        vertx.eventBus().send(Addresses.BROWSER_SPECTATOR_BASE + gameId + "." + spectatorId, startSnapshot);
        data.currentIndex = startSnapshot.getInteger(ROUND_ID);
        data.timeoutStream = vertx.periodicStream(200);

        data.timeoutStream.handler(interval -> {
            if (updates.size() > data.currentIndex + 1) {
                data.currentIndex++;
                vertx.eventBus().send(Addresses.BROWSER_SPECTATOR_BASE  + gameId + "." + spectatorId, updates.get(data.currentIndex));
            } else {
                LOG.warn("Ran out of data to send to " + spectatorId);
            }
        });
    }

    private void handleSnapshots(Message<JsonObject> msg) {
        snapshots.add(msg.body());
        msg.reply(true);
    }

    private void handleUpdates(Message<JsonObject> msg) {
        updates.add(msg.body());
        msg.reply(true);
    }

    private static class SpectatorData {
        TimeoutStream timeoutStream;
        Integer currentIndex;
        Integer startIndex;

        public SpectatorData(Integer startIndex) {
            this.startIndex = startIndex;
        }

    }
}
