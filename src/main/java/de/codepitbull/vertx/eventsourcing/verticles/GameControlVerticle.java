package de.codepitbull.vertx.eventsourcing.verticles;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;

import java.rmi.server.UID;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static de.codepitbull.vertx.eventsourcing.constants.Addresses.*;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.GAME_ID;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.NR_PLAYERS;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.*;
import static de.codeptibull.vertx.kafka.writer.KafkaWriterVerticle.CONFIG_KAFKA_HOST;

/**
 * This verticle takes care of managing game instances.
 * It is responsible for (und)deploying of {@link GameVerticle} and {@link EventStoreVerticle}
 *
 * @author Jochen Mader
 */
public class GameControlVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(GameControlVerticle.class);

    private Map<String, Integer> gameIdToNrOfPlayersMap = new HashMap<>();
    private Map<String, String> gameIdToDeploymentIdMap = new HashMap<>();

    @Override
    public void start() throws Exception {
        vertx.eventBus().localConsumer(GAMES_GET_ONE, this::getGame);
        vertx.eventBus().localConsumer(GAMES_CREATE, this::createGame);
        vertx.eventBus().localConsumer(GAMES_DELETE, this::deleteGame);
        vertx.eventBus().localConsumer(GAMES_LIST, this::listOfGames);
        LOG.info("Deployed "+GameControlVerticle.class.getName());
    }

    /**
     * Called when data about a specific game s received.
     * @param req
     */
    public void getGame(Message<String> req) {
        if(gameIdToNrOfPlayersMap.containsKey(req.body()))
            req.reply(new JsonObject()
                    .put(GAME_ID, req.body())
                    .put(NR_PLAYERS, gameIdToNrOfPlayersMap.get(req.body())));
        else
            req.fail(FAILURE_GAME_DOES_NOT_EXIST.intValue(), "Game doesn't exist");
    }

    /**
     * Called to create a game. Will register the required {@link GameVerticle} and {@link EventStoreVerticle}
     * to handle it.
     * @param req
     */
    public void createGame(Message<Integer> req) {
        String gameId = UUID.randomUUID().toString();
        vertx.deployVerticle(KafkaEventStoreVerticle.class.getName(), new DeploymentOptions().setConfig(
                new JsonObject()
                        .put(GAME_ID, gameId)
                        .put(CONFIG_KAFKA_HOST, "172.16.250.15:9092")
                ), replayDeploymentRes -> {
                //deploy the actual game handling verticle
                vertx.deployVerticle(GameVerticle.class.getName(), new DeploymentOptions().setConfig(
                        new JsonObject()
                                .put(GAME_ID, gameId)
                                .put(NR_PLAYERS, req.body())),
                        gameDeplyomentRes -> {
                            if (gameDeplyomentRes.succeeded()) {
                                gameIdToNrOfPlayersMap.put(gameId, req.body());
                                gameIdToDeploymentIdMap.put(gameId, replayDeploymentRes.result());
                                req.reply(gameId);
                            } else {
                                req.fail(FAILURE_UNABLE_TO_DEPLOY_GAME_VERTICLE.intValue(), "Unable to deploy " + GameVerticle.class.getName());
                                LOG.error("Unable to deploy " + GameVerticle.class.getName(), gameDeplyomentRes.cause());
                            }
                        });
        });
    }

    /**
     * Handles deletion of a game and all associated undeployments.
     * TODO: Not working, see commented code
     * @param req
     */
    public void deleteGame(Message<Integer> req) {
        String deploymentId = gameIdToDeploymentIdMap.remove(req.body());
        gameIdToNrOfPlayersMap.remove(req.body());
        if(deploymentId == null) {
            req.fail(FAILURE_UNABLE_TO_DELETE_NON_EXISTING_GAME.intValue(), "Unable to delete non exiting game.");
        }
        else {
// TODO: Fixed in 3.1: https://bugs.eclipse.org/bugs/show_bug.cgi?id=473778
//            vertx.undeploy(deploymentId, res -> {
//                    if(res.succeeded()) {
//                        req.reply(true);
//                    }
//                    else {
//                        req.fail(FAILURE_UNABLE_TO_UNDEPLOY_GAME_VERTICLE, "Unable to deploy "+GameVerticle.class.getName());
//                        LOG.error("Unable to undeploy " + GameVerticle.class.getName(), res.cause());
//                    }
//                req.reply(true);
//            });
            req.reply(true);
        }
    }

    /**
     * Replies with the list if currently running games.
     * @param req
     */
    public void listOfGames(Message<Integer> req) {
        JsonArray ret = new JsonArray();
        gameIdToNrOfPlayersMap.entrySet().forEach(elem -> ret.add(
                new JsonObject()
                        .put(GAME_ID, elem.getKey())
                        .put(NR_PLAYERS, elem.getValue())));
        req.reply(ret);
    }
}
