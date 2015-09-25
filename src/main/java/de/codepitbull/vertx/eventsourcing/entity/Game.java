package de.codepitbull.vertx.eventsourcing.entity;

import de.codepitbull.vertx.eventsourcing.constants.Constants;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * A small mapper to handle JSON-documents representing a Game.
 *
 * @author Jochen Mader
 */
public class Game {

    private String gameId;
    private Integer roundId;
    private Integer numPlayers;
    private List<Player> players;

    private Game(String gameId, Integer roundId, Integer numPlayers, List<Player> players) {
        this.gameId = gameId;
        this.roundId = roundId;
        this.players = players;
        this.numPlayers = numPlayers;
    }

    public String getGameId() {
        return gameId;
    }

    public Integer getRoundId() {
        return roundId;
    }

    public Integer incrementAndGetRoundId() {
        return ++roundId;
    }

    public Integer getNumPlayers() {
        return numPlayers;
    }

    public List<Player> getPlayers() {
        return players;
    }

    public void addPlayer(Player player) {
        if(players.size() < numPlayers)
            players.add(player);
        else
            throw new IllegalArgumentException("Max number of players reached!");
    }

    public Integer currentNumPlayers() {
        return players.size();
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put(PLAYERS, players.stream().reduce(new JsonArray(), (arr, player) -> arr.add(player.toJson()), (arr1,arr2) -> arr1.addAll(arr2)))
                .put(GAME_ID, gameId)
                .put(ROUND_ID, roundId)
                .put(MSG_TYPE, Constants.SNAPSHOT)
                .put(NR_PLAYERS, numPlayers);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String gameId;
        private Integer roundId = 0;
        private Integer numPlayers;
        private List<Player> players = new ArrayList<>();

        public Builder gameId(String gameId) {
            this.gameId = gameId;
            return this;
        }

        public Builder roundId(Integer roundId) {
            this.roundId = roundId;
            return this;
        }

        public Builder player(Player player) {
            players.add(player);
            return this;
        }

        public Builder numPlayers(Integer numPlayers) {
            this.numPlayers = numPlayers;
            return this;
        }

        public Game build() {
            notNull(gameId, "GameId must not be null");
            notNull(roundId, "RoundId must not be null");
            notNull(players, "Players must not be null");
            notNull(numPlayers, "NumPlayers must not be null");
            return new Game(gameId, roundId, numPlayers, players);
        }
    }
}
