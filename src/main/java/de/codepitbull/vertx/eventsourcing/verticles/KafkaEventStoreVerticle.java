package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.constants.Addresses;
import de.codeptibull.vertx.kafka.simple.KafkaSimpleConsumer;
import de.codeptibull.vertx.kafka.simple.ResultEnum;
import de.codeptibull.vertx.kafka.simple.SimpleConsumerProperties;
import de.codeptibull.vertx.kafka.writer.KafkaWriter;
import de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.TimeoutStream;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.StringDeserializer;
import rx.Observable;

import java.util.*;

import static de.codepitbull.vertx.eventsourcing.constants.Addresses.*;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_MISSING_PARAMETER;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_WRIIING_UPDATE;
import static de.codeptibull.vertx.kafka.simple.KafkaSimpleConsumerVerticle.*;
import static de.codeptibull.vertx.kafka.simple.ResultEnum.OK;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.CONFIG_KAFKA_HOST;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.EVENT;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.TOPIC;
import static org.apache.commons.lang3.Validate.notEmpty;
import static org.apache.commons.lang3.Validate.notNull;
import static rx.observables.JoinObservable.from;
import static rx.observables.JoinObservable.when;

/**
 * Instances of this verticle handle storage and replay of game events.
 *
 * @author Jochen Mader
 */
public class KafkaEventStoreVerticle extends AbstractVerticle{

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEventStoreVerticle.class);

    private String gameId;
    private Integer spectatorCounter = 0;

    private List<JsonObject> snapshots = new ArrayList<>();
    private Map<Integer, SpectatorData> spectatorIdToData= new HashMap<>();

    private KafkaWriter writer;
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        gameId = notNull(config().getString(GAME_ID));

        writer = new KafkaWriter(notEmpty(config().getString(CONFIG_KAFKA_HOST), "kafka_host not set"));

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
        snapshots.add(msg.body());
        msg.reply(true);
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
        writer.write(gameId, msg.body().toString(),
                succ ->
                    msg.reply(true),
                fail -> {
                    msg.fail(FAILURE_WRIIING_UPDATE.intValue(), "Failed writing Update to Event Store");
                    LOG.error("Failed writing Update to Event Store", fail);
                });
    }

    private void handleReplay(JsonObject body) {
        Integer spectatorId = body.getInteger(SPECTATOR_ID);
        LOG.info("Starting to stream game "+gameId+" for spectator "+spectatorId);
        SpectatorData data = spectatorIdToData.get(spectatorId);
        if(data.timeoutStream != null) data.timeoutStream.cancel();
        JsonObject startSnapshot = snapshots.get(data.startIndex);
        vertx.eventBus().send(Addresses.BROWSER_SPECTATOR_BASE  + gameId + "." + spectatorId, startSnapshot);
        data.currentIndex = startSnapshot.getInteger(ROUND_ID);
        data.timeoutStream = vertx.periodicStream(200);

        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(0)
                .port(9092)
                .topic(gameId)
                .addBrokers(Arrays.asList("172.16.250.15".split(",")))
                .offset(data.currentIndex+1)
                .build());

        data.timeoutStream.handler(interval -> {
            vertx.<Pair<ResultEnum, byte[]>>executeBlocking(
                    exe -> exe.complete(consumer.fetch()),
                    res -> vertx.eventBus().send(Addresses.BROWSER_SPECTATOR_BASE  + gameId + "." + spectatorId, new JsonObject(stringDeserializer.deserialize(null, res.result().getRight())))
            );
        });
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
