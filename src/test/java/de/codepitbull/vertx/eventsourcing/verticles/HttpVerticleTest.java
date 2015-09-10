package de.codepitbull.vertx.eventsourcing.verticles;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.CONFIG_PORT;
import static de.codepitbull.vertx.eventsourcing.verticles.GameControlVerticle.ADDRESS_GAMES_CREATE;
import static de.codepitbull.vertx.eventsourcing.verticles.GameControlVerticle.ADDRESS_GAMES_GET_ONE;
import static de.codepitbull.vertx.eventsourcing.verticles.GameVerticle.ADDR_GAME_BASE;
import static de.codepitbull.vertx.eventsourcing.verticles.HttpVerticle.URL_GAMEID;
import static io.vertx.core.http.HttpHeaders.*;

/**
 *
 * @author Jochen Mader
 */
@RunWith(VertxUnitRunner.class)
public class HttpVerticleTest {
    public static final int DEFAULT_PORT = 8077;
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(TestContext ctx) {
        rule.vertx().deployVerticle(HttpVerticle.class.getName(),
                new DeploymentOptions().setConfig(
                        new JsonObject()
                                .put(CONFIG_PORT, DEFAULT_PORT)
                ),
                ctx.asyncAssertSuccess());
    }

    @Test
    public void testCreateAndGetGame(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>localConsumer(ADDRESS_GAMES_CREATE).handler(req -> {
            ctx.assertEquals(2, req.body());
            req.reply(1);
        });

        rule.vertx().eventBus().<Integer>localConsumer(ADDRESS_GAMES_GET_ONE).handler(req -> {
            ctx.assertEquals(1, req.body());
            req.reply(1);
        });

        httpClient()
                .post("/api/games")
                .putHeader(ACCEPT, TEXT_HTML)
                .putHeader("content-type", "application/x-www-form-urlencoded")
                .handler(postResponse -> {
                            ctx.assertEquals(301, postResponse.statusCode());
                            ctx.assertEquals("/api/games/1", postResponse.getHeader(LOCATION.toString()));
                            httpClient()
                                    .get("/api/games/1")
                                    .putHeader(ACCEPT, TEXT_HTML)
                                    .handler(getResponse -> {
                                        ctx.assertEquals(200, getResponse.statusCode());
                                        async.complete();
                                    })
                                    .end();
                        }
                )
                .end("nr_of_players=2");
    }

    @Test
    public void testGetGames(TestContext ctx) {
        Async async = ctx.async();
        httpClient()
                .get("/api/games")
                .putHeader(ACCEPT, TEXT_HTML)
                .handler(response -> {
                            ctx.assertEquals(200, response.statusCode());
                            async.complete();
                        }
                )
                .end();
    }

    @Test
    public void testGetPlayers(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>localConsumer(ADDRESS_GAMES_GET_ONE).handler(req ->
                req.reply(new JsonObject().put(URL_GAMEID, req.body())));

        httpClient()
                .get("/api/games/1/players")
                .putHeader(ACCEPT, TEXT_HTML)
                .handler(response -> {
                            ctx.assertEquals(200, response.statusCode());
                            async.complete();
                        }
                )
                .end();
    }

    @Test
    public void testCreatePlayer(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>localConsumer(ADDR_GAME_BASE + "1").handler(req -> req.reply(2));

        rule.vertx().eventBus().<Integer>consumer(ADDRESS_GAMES_GET_ONE).handler(req ->
            req.reply(new JsonObject().put(URL_GAMEID, req.body())));

        httpClient()
                .post("/api/games/1/players")
                .putHeader(ACCEPT, TEXT_HTML)
                .putHeader("content-type", "application/x-www-form-urlencoded")
                .handler(response -> {
                            ctx.assertEquals(301, response.statusCode());
                            ctx.assertEquals("/api/games/1/players/2", response.getHeader(LOCATION.toString()));
                            async.complete();
                        }
                )
                .end("player_name=player1");
    }

    @Test
    public void testLoadGame(TestContext ctx) {
        Async async = ctx.async();
        rule.vertx().eventBus().<Integer>localConsumer(ADDR_GAME_BASE + "1").handler(req -> req.reply(2));

        rule.vertx().eventBus().<Integer>consumer(ADDRESS_GAMES_GET_ONE).handler(req ->
                req.reply(new JsonObject().put(URL_GAMEID, req.body())));

        httpClient()
                .get("/api/games/1/players/2")
                .putHeader(ACCEPT, TEXT_HTML)
                .handler(response -> {
                            ctx.assertEquals(200, response.statusCode());
                            async.complete();
                        }
                )
                .end("player_name=player1");
    }

    private HttpClient httpClient() {
        return rule.vertx().createHttpClient(new HttpClientOptions().setDefaultPort(DEFAULT_PORT));
    }

}
