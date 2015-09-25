package de.codepitbull.vertx.eventsourcing.store;

import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jmader on 26.09.15.
 */
public class InMemoryStore implements EventStore {
    private List<JsonObject> snapshots = new ArrayList<>();
    private List<JsonObject> updates = new ArrayList<>();

    public void storeUpdate(JsonObject msg) {
        updates.add(msg);
    }

    public void storeSnapshot(JsonObject msg) {
        snapshots.add(msg);
    }

    public JsonObject getSnapshotAtIndex(int index) {
        return snapshots.get(index);
    }

    public JsonObject getUpdateAtIndex(int index) {
        return updates.get(index);
    }
}
