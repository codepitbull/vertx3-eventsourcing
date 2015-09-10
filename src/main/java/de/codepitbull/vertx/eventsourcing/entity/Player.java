package de.codepitbull.vertx.eventsourcing.entity;

import io.vertx.core.json.JsonObject;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * A small mapper to handle JSON-documents representing a Player.
 *
 * @author Jochen Mader
 */
public class Player {

    private JsonObject player;

    private Player(JsonObject player) {
        this.player = player;
    }

    public Integer getX() {
        return player.getInteger(POS_X);
    }

    public void setX(Integer x) {
        player.put(POS_X, x);
    }

    public Integer getY() {
        return player.getInteger(POS_Y);
    }

    public void setY(Integer y) {
        player.put(POS_Y, y);
    }

    public String getName() {
        return player.getString(PLAYER_NAME);
    }

    public Integer getPlayerId() {
        return player.getInteger(PLAYER_ID);
    }

    public JsonObject toJson() {
        return player.copy();
    }

    public static Builder builder() {
        return new Builder();
    };

    public static class Builder {
        private String name;
        private Integer x;
        private Integer y;
        private Integer id;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder x(Integer x) {
            this.x = x;
            return this;
        }

        public Builder y(Integer y) {
            this.y = y;
            return this;
        }

        public Builder id(Integer id) {
            this.id = id;
            return this;
        }

        public Player build() {
            notNull(name, "Name must not be null");
            notNull(x, "X must not be null");
            notNull(y, "Y must not be null");
            notNull(id, "ID must not be null");
            return new Player(new JsonObject()
                    .put(PLAYER_ID, id)
                    .put(POS_X, x)
                    .put(POS_Y, y)
                    .put(PLAYER_NAME, name));
        }
    }
}
