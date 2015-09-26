package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.constants.Addresses;
import de.codepitbull.vertx.eventsourcing.store.EventStore;
import de.codepitbull.vertx.eventsourcing.store.InMemoryEventStore;
import de.codepitbull.vertx.eventsourcing.store.KafkaEventStore;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.TimeoutStream;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;

import java.util.HashMap;
import java.util.Map;

import static de.codepitbull.vertx.eventsourcing.constants.Addresses.*;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_MISSING_PARAMETER;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_WRIIING_UPDATE;
import static org.apache.commons.lang3.Validate.notNull;
import static rx.observables.JoinObservable.from;
import static rx.observables.JoinObservable.when;

/**
 * Instances of this verticle handle storage and replay of game events.
 *
 * @author Jochen Mader
 */
public class EventStoreVerticle extends AbstractVerticle{

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreVerticle.class);

    private String gameId;
    private Integer spectatorCounter = 0;

    private Map<Integer, SpectatorData> spectatorIdToData= new HashMap<>();

    private EventStore eventStore;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        gameId = notNull(config().getString(GAME_ID));

        if(config().getBoolean(CONFIG_IN_MEM, true)) {
            eventStore = new InMemoryEventStore();
            LOG.info("Using "+InMemoryEventStore.class.getName());
        }
        else {
            eventStore = new KafkaEventStore(vertx, gameId, config().getString(CONFIG_KAFKA_HOST), config().getInteger(CONFIG_KAFKA_PORT));
            LOG.info("Using "+KafkaEventStore.class.getName());
        }

        MessageConsumer<JsonObject> updatesConsumer = vertx.eventBus().<JsonObject>consumer(REPLAY_UPDATES_BASE + gameId);
        updatesConsumer.handler(this::handleUpdates);

        MessageConsumer<JsonObject> snapshotsConsumer = vertx.eventBus().<JsonObject>consumer(REPLAY_SNAPSHOTS_BASE + gameId);
        snapshotsConsumer.handler(this::handleSnapshots);

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

    private void handleSnapshots(Message<JsonObject> msg) {
        eventStore.storeSnapshot(msg.body(), result -> {
            if (result.succeeded())
                msg.reply(true);
            else {
                msg.fail(FAILURE_WRIIING_UPDATE.intValue(), "Failed writing Update to Event Store");
                LOG.error("Failed writing Update to Event Store", result.cause());
            }
        });
    }

    private void handleConsumerRegistration(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        if(body.containsKey(REPLAY_INDEX)) {
            spectatorIdToData.put(++spectatorCounter, new SpectatorData(body.getInteger(REPLAY_INDEX)));
            msg.reply(spectatorCounter);
        }
        else msg.fail(FAILURE_MISSING_PARAMETER.intValue(), "Missing "+ REPLAY_INDEX);
    }

    private void handleUpdates(Message<JsonObject> msg) {
        eventStore.storeEvent(msg.body(), result -> {
            if (result.succeeded())
                msg.reply(true);
            else {
                msg.fail(FAILURE_WRIIING_UPDATE.intValue(), "Failed writing Update to Event Store");
                LOG.error("Failed writing Update to Event Store", result.cause());
            }
        });
    }

    private void handleReplay(JsonObject body) {
        Integer spectatorId = body.getInteger(SPECTATOR_ID);
        LOG.info("Starting to stream game "+gameId+" for spectator "+spectatorId);
        SpectatorData data = spectatorIdToData.get(spectatorId);
        if(data.timeoutStream != null) data.timeoutStream.cancel();
        eventStore.loadSnapshot(data.startIndex, result -> {
            if (result.succeeded()) {
                JsonObject startSnapshot = result.result();
                vertx.eventBus().send(Addresses.BROWSER_SPECTATOR_BASE + gameId + "." + spectatorId, startSnapshot);
                data.timeoutStream = vertx.periodicStream(200);
                data.startIndex = startSnapshot.getInteger(ROUND_ID);

                eventStore.startConsumerForSpectatorIdWithStartOffset(spectatorId, data.startIndex);

                data.timeoutStream.handler(interval ->
                        eventStore.getNextEvent(spectatorId, res -> {
                            if (res.succeeded())
                                vertx.eventBus().send(Addresses.BROWSER_SPECTATOR_BASE + gameId + "." + spectatorId, res.result());
                            else
                                LOG.error("Unable to load update", res.cause());
                        }));
            } else {
                LOG.error("Unable to load snapshot", result.cause());
            }
        });
    }

    private static class SpectatorData {
        TimeoutStream timeoutStream;
        Integer startIndex;

        public SpectatorData(Integer startIndex) {
            this.startIndex = startIndex;
        }

    }
}
