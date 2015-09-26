package de.codepitbull.vertx.eventsourcing.store;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * An in-memory event store for playing around.
 */
public class InMemoryEventStore implements EventStore {

    private List<JsonObject> snapshots = new ArrayList<>();
    private List<JsonObject> updates = new ArrayList<>();
    private Map<Integer, MutableInt> spectatorIdToIndexMap = new HashMap<>();


    @Override
    public void storeEvent(JsonObject event, Handler<AsyncResult<Void>> resultHandler) {
        updates.add(event);
        resultHandler.handle(succeededFuture());
    }

    @Override
    public void storeSnapshot(JsonObject event, Handler<AsyncResult<Void>> resultHandler) {
        snapshots.add(event);
        resultHandler.handle(succeededFuture());
    }

    @Override
    public void loadSnapshot(int index, Handler<AsyncResult<JsonObject>> resultHandler) {
        if (index < snapshots.size())
            resultHandler.handle(succeededFuture(snapshots.get(index)));
        else
            resultHandler.handle(failedFuture("No Snapshot " + index));
    }

    @Override
    public void getNextEvent(Integer spectatorId, Handler<AsyncResult<JsonObject>> resultHandler) {
        if (spectatorIdToIndexMap.containsKey(spectatorId)) {
            spectatorIdToIndexMap.get(spectatorId).increment();
            resultHandler.handle(succeededFuture(updates.get(spectatorIdToIndexMap.get(spectatorId).getValue())));
        } else
            resultHandler.handle(failedFuture("No spectator with id " + spectatorId + " registered"));
    }

    @Override
    public void startConsumerForSpectatorIdWithStartOffset(Integer spectatorId, Integer startOffset) {
        stopConsumerForSpectatorId(spectatorId);
        spectatorIdToIndexMap.put(spectatorId, new MutableInt(startOffset));
    }

    @Override
    public void stopConsumerForSpectatorId(Integer spectatorId) {
        spectatorIdToIndexMap.remove(spectatorId);
    }

}
