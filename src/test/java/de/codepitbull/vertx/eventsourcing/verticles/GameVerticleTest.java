package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.constants.Addresses;
import de.codepitbull.vertx.eventsourcing.constants.Constants;
import de.codepitbull.vertx.eventsourcing.entity.Game;
import de.codepitbull.vertx.eventsourcing.entity.Player;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;

/**
 *
 * @author Jochen Mader
 */
@RunWith(VertxUnitRunner.class)
public class GameVerticleTest {
    public static final String DEFAULT_GAME_ID = "1";
    public static final int DEFAULT_NR_PLAYERS = 2;
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(TestContext ctx) {
        rule.vertx().deployVerticle(GameVerticle.class.getName(),
                new DeploymentOptions().setConfig(
                        new JsonObject()
                                .put(GAME_ID, DEFAULT_GAME_ID)
                                .put(NR_PLAYERS, DEFAULT_NR_PLAYERS)
                ),
                ctx.asyncAssertSuccess());
    }

    @Test
    public void testSetPlayerNameAndGetPlayerId(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>send(Addresses.GAME_BASE + DEFAULT_GAME_ID,
                new JsonObject()
                        .put(Constants.ACTION, Constants.ACTION_REG)
                        .put(Constants.PLAYER_NAME, "player1")
                , resp -> {
                    if (resp.failed()) ctx.fail(resp.cause());
                    else ctx.assertEquals(0, resp.result().body());
                    async.complete();
                });
    }

    @Test
    public void testGetSnapshot(TestContext ctx) {
        Async async = ctx.async();
        //name player1
        rule.vertx().eventBus().<Integer>send(Addresses.GAME_BASE + DEFAULT_GAME_ID,
                new JsonObject()
                        .put(Constants.ACTION, Constants.ACTION_REG)
                        .put(Constants.PLAYER_NAME, "player1")
                , resp -> {
                    ctx.assertEquals(0, resp.result().body());
                    //Request game snapshot
                    rule.vertx().eventBus().<JsonObject>send(Addresses.GAME_BASE + DEFAULT_GAME_ID, new JsonObject().put(Constants.ACTION, Constants.ACTION_SNAPSHOT), snapshot -> {
                        JsonObject gameJson = Game.builder()
                                .roundId(0).numPlayers(2).gameId("1")
                                .player(Player.builder().id(0).name("player1").x(3).y(5).build())
                                .build().toJson();
                        ctx.assertEquals(gameJson, snapshot.result().body());
                        async.complete();
                    });
                });
    }

    @Test
    public void testJoinAndStartAndPlayGame(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().localConsumer(Addresses.REPLAY_UPDATES_BASE + DEFAULT_GAME_ID).handler(req -> req.reply(true));
        rule.vertx().eventBus().localConsumer(Addresses.REPLAY_SNAPSHOTS_BASE + DEFAULT_GAME_ID).handler(req -> req.reply(true));
        rule.vertx().eventBus().<JsonObject>localConsumer(Addresses.BROWSER_GAME_BASE + "1").handler(req -> {
            //wait for a response to our move-left-command
            if(req.body().containsKey(PLAYERS) && req.body().getJsonArray(PLAYERS).size() == 1) {
                JsonObject player = req.body().getJsonArray(PLAYERS).getJsonObject(0);
                ctx.assertNotEquals(0, player.getInteger(ROUND_ID));
                ctx.assertEquals(0, player.getInteger(PLAYER_ID));
                ctx.assertEquals("l", player.getString("mov"));
                async.complete();
            }
        });

        rule.vertx().eventBus().<Integer>send(Addresses.GAME_BASE + DEFAULT_GAME_ID,
                new JsonObject()
                        .put(Constants.ACTION, Constants.ACTION_REG)
                        .put(Constants.PLAYER_NAME, "player1"), pl1Registered -> rule.vertx().eventBus().<Integer>send(Addresses.GAME_BASE + DEFAULT_GAME_ID,
                        new JsonObject()
                                .put(Constants.ACTION, Constants.ACTION_REG)
                                .put(Constants.PLAYER_NAME, "player2"), pl2Registered ->
                                //send move-left-command for player1
                                rule.vertx().eventBus().send(Addresses.GAME_BASE + DEFAULT_GAME_ID,
                                        new JsonObject()
                                                .put(Constants.ACTION, Constants.ACTION_MOVE)
                                                .put(PLAYER_ID, 0).put(Constants.ACTION_MOVE, "l"))));

    }
}
