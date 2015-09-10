package de.codepitbull.vertx.eventsourcing.verticles;

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

import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_MISSING_PARAMETER;
import static org.apache.commons.lang3.Validate.notNull;
import static rx.observables.JoinObservable.from;
import static rx.observables.JoinObservable.when;

/**
 * Instances of this verticle handle the replay of a specific game.
 *
 * @author Jochen Mader
 */
public class ReplayVerticle extends AbstractVerticle{

    private static final Logger LOG = LoggerFactory.getLogger(ReplayVerticle.class);

    public static final String ADDR_REPLAY_REGISTER_BASE = "replay.register.";
    public static final String ADDR_REPLAY_SNAPSHOTS_BASE = "replay.snapshots.";
    public static final String ADDR_REPLAY_UPDATES_BASE = "replay.updates.";
    public static final String ADDR_REPLAY_START_BASE = "replay.start.";
    public static final String ADDR_BROWSER_SPECTATOR_BASE = "browser.replay.";

    private Integer gameId;
    private Integer spectatorCounter = 0;

    private List<JsonObject> snapshots = new ArrayList<>();
    private List<JsonObject> updates = new ArrayList<>();
    private Map<Integer, SpectatorData> spectatorIdToData= new HashMap<>();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        gameId = notNull(config().getInteger(GAME_ID));

        MessageConsumer<JsonObject> registerConsumer = vertx.eventBus().<JsonObject>consumer(ADDR_REPLAY_REGISTER_BASE + gameId);
        registerConsumer.handler(this::registerConsumer);
        MessageConsumer<JsonObject> snapshotsConsumer = vertx.eventBus().<JsonObject>consumer(ADDR_REPLAY_SNAPSHOTS_BASE + gameId);
        snapshotsConsumer.bodyStream().handler(body -> snapshots.add(body));
        MessageConsumer<JsonObject> updatesConsumer = vertx.eventBus().<JsonObject>consumer(ADDR_REPLAY_UPDATES_BASE + gameId);
        updatesConsumer.bodyStream().handler(body -> updates.add(body));
        MessageConsumer<JsonObject> startConsumer = vertx.eventBus().<JsonObject>consumer(ADDR_REPLAY_START_BASE + gameId);
        startConsumer.bodyStream()
                .handler(body -> {
                    Integer spectatorId = body.getInteger(SPECTATOR_ID);
                    LOG.info("Starting to stream game "+gameId+" for spectator "+spectatorId);
                    SpectatorData data = spectatorIdToData.get(spectatorId);
                    if(data.timeoutStream != null) data.timeoutStream.cancel();
                    JsonObject startSnapshot = snapshots.get(data.startIndex);
                    vertx.eventBus().send(ADDR_BROWSER_SPECTATOR_BASE + spectatorId, startSnapshot);
                    data.currentIndex = startSnapshot.getInteger(ROUND_ID);
                    data.timeoutStream = vertx.periodicStream(200);

                    data.timeoutStream.handler(interval -> {
                        if (updates.size() > data.currentIndex + 1) {
                            data.currentIndex++;
                            vertx.eventBus().send(ADDR_BROWSER_SPECTATOR_BASE + spectatorId, updates.get(data.currentIndex));
                        } else {
                            LOG.warn("Ran out of data to send to "+spectatorId);
                        }
                    });
                });

        when(
            from(registerConsumer.completionHandlerObservable())
            .and(snapshotsConsumer.completionHandlerObservable())
            .and(updatesConsumer.completionHandlerObservable())
                    .and(startConsumer.completionHandlerObservable())
                    .then((a, b, c, d) -> null)
        ).toObservable().subscribe(
                success -> {
                    LOG.info("Succeeded deploying " + ReplayVerticle.class);
                    startFuture.complete();
                },
                failure -> {
                    LOG.info("Failed deploying " + ReplayVerticle.class, failure);
                    startFuture.fail(failure);
                }
        );

    }

    private void registerConsumer(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        if(body.containsKey(REPLAY_INDEX)) {
            spectatorIdToData.put(++spectatorCounter, new SpectatorData(body.getInteger(REPLAY_INDEX)));
            msg.reply(spectatorCounter);
        }
        else msg.fail(FAILURE_MISSING_PARAMETER.intValue(), "Missing "+ REPLAY_INDEX);
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
