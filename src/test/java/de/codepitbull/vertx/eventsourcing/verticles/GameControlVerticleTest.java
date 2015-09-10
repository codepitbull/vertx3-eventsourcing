package de.codepitbull.vertx.eventsourcing.verticles;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.GAME_ID;
import static de.codepitbull.vertx.eventsourcing.constants.Constants.NR_PLAYERS;
import static de.codepitbull.vertx.eventsourcing.verticles.GameControlVerticle.*;

/**
 *
 * @author Jochen Mader
 */
@RunWith(VertxUnitRunner.class)
public class GameControlVerticleTest {
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(TestContext ctx) {
        rule.vertx().deployVerticle(GameControlVerticle.class.getName(), ctx.asyncAssertSuccess());
    }

    @Test
    public void testCreateGame(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_CREATE, 4, resp -> {
            ctx.assertEquals(1, resp.result().body());
            async.complete();
        });
    }

    @Test
    public void testDeleteGame(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_CREATE, 4, respCreate -> {
            ctx.assertEquals(1, respCreate.result().body());
            rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_DELETE, 1, respDelete -> {
                ctx.assertEquals(true, respDelete.result().body());
                async.complete();
            });
        });
    }

    @Test
    public void testGetOneGame(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_CREATE, 4, respCreate -> {
            ctx.assertEquals(1, respCreate.result().body());
            rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_GET_ONE, 1, respDelete -> {
                JsonObject game = new JsonObject()
                        .put(GAME_ID, 1)
                        .put(NR_PLAYERS, 4);
                ctx.assertEquals(game, respDelete.result().body());
                async.complete();
            });
        });
    }

    @Test
    public void testListGames(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_CREATE, 4, respCreate -> {
            ctx.assertEquals(1, respCreate.result().body());
            rule.vertx().eventBus().<Integer>send(ADDRESS_GAMES_LIST, 1, respDelete -> {
                JsonArray comp = new JsonArray().add(new JsonObject()
                        .put(GAME_ID, 1)
                        .put(NR_PLAYERS, 4));
                ctx.assertEquals(comp, respDelete.result().body());
                async.complete();
            });
        });
    }
}
