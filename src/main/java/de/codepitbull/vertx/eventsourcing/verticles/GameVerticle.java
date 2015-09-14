package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.entity.Game;
import de.codepitbull.vertx.eventsourcing.entity.Player;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import rx.observables.ConnectableObservable;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static de.codepitbull.vertx.eventsourcing.constants.Addresses.*;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.FAILURE_GAME_FULL;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.PLAYERS;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.ROUND_ID;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.PLAYER_ID;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * Instances of this verticle handle the whole game logic.
 *
 * @author Jochen Mader
 */
public class GameVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(GameVerticle.class);

    private Game game;

    private JsonArray additionalActions = new JsonArray();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        game = Game.builder()
                .gameId(notNull(config().getInteger(GAME_ID)))
                .numPlayers(notNull(config().getInteger(NR_PLAYERS)))
                .roundId(0)
                .build();

        MessageConsumer<JsonObject> gameConsumer = vertx.eventBus().<JsonObject>consumer(GAME_BASE + game.getGameId());

        ConnectableObservable<Message<JsonObject>> gameConsumerObservable = gameConsumer.toObservable().publish();

        gameConsumerObservable.filter(msg -> ACTION_REG.equals(msg.body().getString(ACTION)))
                .forEach(this::registerPlayer);

        gameConsumerObservable.filter(msg -> ACTION_MOVE.equals(msg.body().getString(ACTION)))
                .buffer(200, TimeUnit.MILLISECONDS)
                .forEach(this::playerAction);

        gameConsumerObservable.filter(msg -> ACTION_SNAPSHOT.equals(msg.body().getString(ACTION)))
                .forEach(msg -> msg.reply(game.toJson()));

        gameConsumerObservable.connect();

        //send periodic updates
        vertx.eventBus().publish(REPLAY_SNAPSHOTS_BASE + game.getGameId(), game.toJson());
        vertx.setPeriodic(2000, time -> {
            vertx.eventBus().send(REPLAY_SNAPSHOTS_BASE + game.getGameId(), game.toJson(), new DeliveryOptions().setSendTimeout(30),
                    result -> {
                        if(result.failed())
                            LOG.error("Failed storing Snapshot", result.cause());
                    });
        });

        gameConsumer.completionHandlerObservable().subscribe(
                success -> {
                    LOG.info("Succeeded deploying " + GameVerticle.class + " for game " + game.getGameId());
                    startFuture.complete();
                },
                failure -> {
                    LOG.info("Failed deploying " + GameVerticle.class + " for game " + game.getGameId(), failure);
                    startFuture.fail(failure);
                }
        );
    }

    /**
     * Called whenever a round has elapsed to process all commands that piled up in the mean time.
     * @param msgs
     */
    public void playerAction(List<Message<JsonObject>> msgs) {
        JsonArray movedPlayers = msgs.stream()
                .map(msg -> msg.body())
                .reduce(new JsonArray(),
                        (jArray, msg) -> {
                            Player player = game.getPlayers().get(msg.getInteger(PLAYER_ID));
                            switch (msg.getString(ACTION_MOVE)) {
                                case "l":
                                    player.setX(player.getX() - 1);
                                    break;
                                case "r":
                                    player.setX(player.getX() + 1);
                                    break;
                                case "u":
                                    player.setY(player.getY() - 1);
                                    break;
                                case "d":
                                    player.setY(player.getY() + 1);
                                    break;
                            }
                            return jArray.add(new JsonObject()
                                    .put(PLAYER_ID, player.getPlayerId())
                                    .put(ACTION_MOVE, msg.getString(ACTION_MOVE)));
                        },
                        (arr1, arr2) -> arr1.addAll(arr2)
                );
        JsonObject update = new JsonObject()
                .put(PLAYERS, movedPlayers)
                .put(ROUND_ID, game.incrementAndGetRoundId())
                .put(ACTIONS, additionalActions.copy());

        // send updates
        vertx.eventBus().send(REPLAY_UPDATES_BASE + game.getGameId(), update, new DeliveryOptions().setSendTimeout(30),
                result -> {
                    if (result.succeeded())
                        vertx.eventBus().publish(BROWSER_GAME_BASE + game.getGameId(), update);
                    else
                        LOG.error("Failed storing event", result.cause());
                });
        additionalActions.clear();
    }

    /**
     * Called when an event for player registration has been received.
     * @param msg
     */
    public void registerPlayer(Message<JsonObject> msg) {
        if (game.currentNumPlayers() < game.getNumPlayers()) {
            Integer playerId = game.currentNumPlayers();
            Player newPlayer = Player.builder()
                    .id(playerId)
                    .name(msg.body().getString(PLAYER_NAME))
                    .x(playerId + 3)
                    .y(5)
                    .build();
            game.addPlayer(newPlayer);
            additionalActions.add(new JsonObject()
                            .put(ACTION, ACTION_NEW_PLAYER)
                            .put(PLAYER, newPlayer.toJson())
            );
            LOG.info("Added Player "+newPlayer.getName()+" with id "+playerId);
            msg.reply(playerId);
        } else {
            LOG.info("Failed Adding Player because game is full.");
            msg.fail(FAILURE_GAME_FULL.intValue(), "Game is full!");
        }
    }

}
