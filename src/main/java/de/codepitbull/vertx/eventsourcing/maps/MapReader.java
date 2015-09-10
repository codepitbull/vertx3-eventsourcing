package de.codepitbull.vertx.eventsourcing.maps;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import static rx.Observable.*;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * Reads a given map and provides the data as JSON-documents. Will be used for server side collision checking.
 *
 * @author Jochen Mader
 */
public class MapReader {
    private JsonObject jsonObject;

    private Map<String, JsonArray> nameToDataMap;

    public MapReader(JsonObject jsonObject) {
        notNull(jsonObject);
        this.jsonObject = jsonObject;
        nameToDataMap = new HashMap<>();
        layers().forEach(obj -> nameToDataMap.put(((JsonObject)obj).getString("name"), ((JsonObject)obj).getJsonArray("data")));
    }

    public Integer height() {
        return jsonObject.getInteger("height");
    }

    public Integer width() {
        return jsonObject.getInteger("width");
    }

    public Integer tileheight() {
        return jsonObject.getInteger("tileheight");
    }

    public Integer tilewidth() {
        return jsonObject.getInteger("tilewidth");
    }

    public JsonArray layers() {
        return jsonObject.getJsonArray("layers");
    }

    public JsonArray floor() {
        return nameToDataMap.get("floor");
    }

    public JsonArray walls() {
        return nameToDataMap.get("walls");
    }

    public JsonArray furniture() {
        return nameToDataMap.get("furniture");
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("height: ").append(height()).append("\n")
            .append("width: ").append(width()).append("\n")
            .append("tileheight: ").append(tileheight()).append("\n")
            .append("tilewidth: ").append(tilewidth()).append("\n")
            .append("floor: \n");
        from(floor()).buffer(width()).forEach(val -> builder.append(val.toString()).append("\n"));
        builder.append("walls: \n");
        from(walls()).buffer(width()).forEach(val -> builder.append(val.toString()).append("\n"));
        builder.append("furniture: \n");
        from(furniture()).buffer(width()).forEach(val -> builder.append(val.toString()).append("\n"));
        return builder.toString();
    }
}
