package de.codepitbull.vertx.eventsourcing.store;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Future;
import io.vertx.rxjava.core.eventbus.Message;
import scala.Int;

import java.util.Optional;

/**
 * A basic event store abstraction. Each method has to be implemented so that it doesn't block.
 */
public interface EventStore {

    void storeEvent(JsonObject event, Handler<AsyncResult<Void>> resultHandler);

    void storeSnapshot(JsonObject event, Handler<AsyncResult<Void>> resultHandler);

    void loadSnapshot(int index, Handler<AsyncResult<JsonObject>> resultHandler);

    void getNextEvent(Integer spectatorId, Handler<AsyncResult<JsonObject>> resultHandler);

    void startConsumerForSpectatorIdWithStartOffset(Integer spectatorId, Integer startOffset);

    void stopConsumerForSpectatorId(Integer spectatorId);
}
