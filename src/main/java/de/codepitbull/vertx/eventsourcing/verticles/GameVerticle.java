package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.PlayerActionHandler;
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
import rx.Observable;
import rx.functions.Action1;
import rx.observables.ConnectableObservable;

import java.util.HashMap;
import java.util.Map;
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

    private Map<String, PlayerActionHandler> actionToHandlerMap = new HashMap<>();
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        actionToHandlerMap.put(MOVE_LEFT, player -> player.setX(player.getX() - 1));
        actionToHandlerMap.put(MOVE_RIGHT, player -> player.setX(player.getX() + 1));
        actionToHandlerMap.put(MOVE_UP, player -> player.setY(player.getY() - 1));
        actionToHandlerMap.put(MOVE_DOWN, player -> player.setY(player.getY() + 1));

        game = Game.builder()
                .gameId(notNull(config().getString(GAME_ID)))
                .numPlayers(notNull(config().getInteger(NR_PLAYERS)))
                .roundId(0)
                .build();

        MessageConsumer<JsonObject> gameConsumer = vertx.eventBus().<JsonObject>consumer(GAME_BASE + game.getGameId());

        ConnectableObservable<Message<JsonObject>> gameConsumerObservable = gameConsumer.toObservable().publish();

        gameConsumerObservable.filter(msg -> ACTION_REG.equals(msg.body().getString(ACTION)))
                .forEach(this::registerPlayer);

        gameConsumerObservable.filter(msg -> ACTION_MOVE.equals(msg.body().getString(ACTION)))
                .window(200, TimeUnit.MILLISECONDS)
                .forEach(processRound()
        );

        gameConsumerObservable.filter(msg -> ACTION_SNAPSHOT.equals(msg.body().getString(ACTION)))
                .forEach(msg -> msg.reply(game.toJson()));

        gameConsumerObservable.connect();

        //send initial worldstate
        vertx.eventBus().publish(REPLAY_SNAPSHOTS_BASE + game.getGameId(), game.toJson());

        //send periodic worldstate snapshots
        vertx.setPeriodic(2000, time -> {
            vertx.eventBus().send(REPLAY_SNAPSHOTS_BASE + game.getGameId(), game.toJson(), new DeliveryOptions().setSendTimeout(30),
                    result -> {
                        if (result.failed())
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

    private Action1<Observable<Message<JsonObject>>> processRound() {
        return m ->
            m.collect(() -> new JsonArray(),
                    (jArray, msg) -> {
                        JsonObject body = msg.body();
                        Player player = game.getPlayers().get(body.getInteger(PLAYER_ID));
                        actionToHandlerMap.get(body.getString(ACTION_MOVE)).action(player);
                        jArray.add(new JsonObject()
                                .put(PLAYER_ID, player.getPlayerId())
                                .put(ACTION_MOVE, body.getString(ACTION_MOVE)));
                    }
            )
            .forEach(movedPlayers -> {
                JsonObject update = new JsonObject()
                        .put(PLAYERS, movedPlayers)
                        .put(ROUND_ID, game.incrementAndGetRoundId())
                        .put(ACTIONS, additionalActions.copy());
                additionalActions.clear();
                vertx.eventBus().send(REPLAY_UPDATES_BASE + game.getGameId(), update,
                        result -> {
                            if (result.succeeded())
                                vertx.eventBus().publish(BROWSER_GAME_BASE + game.getGameId(), update);
                            else {
                                LOG.error("Failed storing " + update, result.cause());
                            }
                        });
            });
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
