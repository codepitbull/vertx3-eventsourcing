package de.codepitbull.vertx.eventsourcing.verticles;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;

import java.util.HashMap;
import java.util.Map;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.GAME_ID;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.NR_PLAYERS;
import static de.codepitbull.vertx.eventsourcing.constants.FailureCodesEnum.*;

/**
 * This verticle takes care of managing game instances.
 * It is responsible for (und)deploying of {@link GameVerticle} and {@link ReplayVerticle}
 *
 * @author Jochen Mader
 */
public class GameControlVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(GameControlVerticle.class);

    public static final String ADDRESS_GAMES_CREATE = "games.create";
    public static final String ADDRESS_GAMES_GET_ONE = "games.get";
    public static final String ADDRESS_GAMES_DELETE = "games.delete";
    public static final String ADDRESS_GAMES_LIST = "games.list";

    private Integer gameCounter = 0;
    private Map<Integer, Integer> gameIdToNrOfPlayersMap = new HashMap<>();
    private Map<Integer, String> gameIdToDeploymentIdMap = new HashMap<>();

    @Override
    public void start() throws Exception {
        vertx.eventBus().localConsumer(ADDRESS_GAMES_GET_ONE, this::getGame);
        vertx.eventBus().localConsumer(ADDRESS_GAMES_CREATE, this::createGame);
        vertx.eventBus().localConsumer(ADDRESS_GAMES_DELETE, this::deleteGame);
        vertx.eventBus().localConsumer(ADDRESS_GAMES_LIST, this::listOfGames);
        LOG.info("Deployed "+GameControlVerticle.class.getName());
    }

    public void getGame(Message<Integer> req) {
        if(gameIdToNrOfPlayersMap.containsKey(req.body()))
            req.reply(new JsonObject()
                    .put(GAME_ID, req.body())
                    .put(NR_PLAYERS, gameIdToNrOfPlayersMap.get(req.body())));
        else
            req.fail(FAILURE_GAME_DOES_NOT_EXIST.intValue(), "Game doesn't exist");
    }

    public void createGame(Message<Integer> req) {
        int gameId = ++gameCounter;
        //deploy verticle for game replay
        vertx.deployVerticle(ReplayVerticle.class.getName(), new DeploymentOptions().setConfig(
                new JsonObject()
                        .put(GAME_ID, gameId)), replayDeploymentRes -> {
                //deploythe actual game handling verticle
                vertx.deployVerticle(GameVerticle.class.getName(), new DeploymentOptions().setConfig(
                        new JsonObject()
                                .put(GAME_ID, gameId)
                                .put(NR_PLAYERS, req.body())), gameDeplyomentRes -> {
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

    public void listOfGames(Message<Integer> req) {
        JsonArray ret = new JsonArray();
        gameIdToNrOfPlayersMap.entrySet().forEach(elem -> ret.add(
                new JsonObject()
                        .put(GAME_ID, elem.getKey())
                        .put(NR_PLAYERS, elem.getValue())));
        req.reply(ret);
    }
}
