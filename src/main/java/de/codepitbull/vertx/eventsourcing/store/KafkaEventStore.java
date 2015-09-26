package de.codepitbull.vertx.eventsourcing.store;

import de.codeptibull.vertx.kafka.simple.KafkaSimpleConsumer;
import de.codeptibull.vertx.kafka.simple.SimpleConsumerProperties;
import de.codeptibull.vertx.kafka.writer.KafkaWriter;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Context;
import io.vertx.rxjava.core.Vertx;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.Validate.notEmpty;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * A Kafka based event store.
 */
public class KafkaEventStore implements EventStore{

    private KafkaWriter writer;
    private List<JsonObject> snapshots = new ArrayList<>();
    private Map<Integer, KafkaSimpleConsumer> spectatorIdToConsumerMap = new HashMap<>();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private String gameId;
    private Vertx vertx;
    private String kafkaHost;
    private Integer kafkaPort;

    public KafkaEventStore(Vertx vertx, String gameId, String kafkaHost, Integer kafkaPort) {
        this.kafkaHost = notEmpty(kafkaHost, "kafka_host not set");
        this.kafkaPort = notNull(kafkaPort, "kafka_host not set");
        this.gameId = notEmpty(gameId, "gameId must be set");
        writer = new KafkaWriter(kafkaHost+":"+kafkaPort);
        this.vertx = vertx;
    }

    @Override
    public void storeEvent(JsonObject event, Handler<AsyncResult<Void>> resultHandler) {
        Context ctx = vertx.getOrCreateContext();
        writer.write(gameId, event.toString(),
                succ ->
                    ctx.runOnContext(v -> resultHandler.handle(Future.<Void>succeededFuture())),
                fail ->
                    ctx.runOnContext(v -> resultHandler.handle(Future.<Void>failedFuture(fail))));
    };

    @Override
    public void storeSnapshot(JsonObject event, Handler<AsyncResult<Void>> resultHandler) {
        snapshots.add(event);
        resultHandler.handle(Future.<Void>succeededFuture());
    }

    @Override
    public void loadSnapshot(int index, Handler<AsyncResult<JsonObject>> resultHandler) {
        if(index<snapshots.size())
            resultHandler.handle(Future.succeededFuture(snapshots.get(index)));
        else
            resultHandler.handle(Future.failedFuture("No snapshot with index "+index));
    }

    @Override
    public void getNextEvent(Integer spectatorId, Handler<AsyncResult<JsonObject>> resultHandler) {
        vertx.executeBlocking(
                exe -> exe.complete(new JsonObject(stringDeserializer.deserialize(null, spectatorIdToConsumerMap.get(spectatorId).fetch().getRight()))),
                resultHandler);
    }

    @Override
    public void startConsumerForSpectatorIdWithStartOffset(Integer spectatorId, Integer startOffset) {
        stopConsumerForSpectatorId(spectatorId);
        List<String> brokers = new ArrayList<>();
        brokers.add(kafkaHost);
        spectatorIdToConsumerMap.put(spectatorId, new KafkaSimpleConsumer(new SimpleConsumerProperties.Builder()
                .partition(0)
                .port(kafkaPort)
                .topic(gameId)
                .addBrokers(brokers)
                .offset(startOffset+1)
                .build()));
    }

    @Override
    public void stopConsumerForSpectatorId(Integer spectatorId) {
        if(spectatorIdToConsumerMap.containsKey(spectatorId))
            spectatorIdToConsumerMap.remove(spectatorId).close();
    }
}
