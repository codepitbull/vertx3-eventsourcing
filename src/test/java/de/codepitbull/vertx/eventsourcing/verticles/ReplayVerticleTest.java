package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.constants.Constants;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.EventBus;
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
import static de.codepitbull.vertx.eventsourcing.constants.Constants.ROUND_ID;
import static de.codepitbull.vertx.eventsourcing.verticles.ReplayVerticle.*;
import static java.util.stream.IntStream.range;

/**
 *
 * @author Jochen Mader
 */
@RunWith(VertxUnitRunner.class)
public class ReplayVerticleTest {
    public static final int DEFAULT_GAME_ID = 1;
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    private EventBus eventBus;

    @Before
    public void setUp(TestContext ctx) {
        rule.vertx().deployVerticle(ReplayVerticle.class.getName(),
                new DeploymentOptions().setConfig(
                        new JsonObject()
                                .put(GAME_ID, DEFAULT_GAME_ID)
                ),
                ctx.asyncAssertSuccess());
        eventBus = rule.vertx().eventBus();
    }

    @Test
    public void testRegisterConsumer(TestContext ctx) {
        Async async = ctx.async();
        eventBus.<Integer>send(ADDR_REPLAY_REGISTER_BASE + DEFAULT_GAME_ID, new JsonObject().put(Constants.REPLAY_INDEX, 4), response -> {
            ctx.assertTrue(response.succeeded());
            ctx.assertEquals(1, response.result().body());
            async.complete();
        });
    }

    @Test
    public void testReplay(TestContext ctx) {
        Async async = ctx.async();
        range(0, 10).forEach(val -> eventBus.send(ADDR_REPLAY_SNAPSHOTS_BASE + DEFAULT_GAME_ID, new JsonObject().put("sn-id", val).put(ROUND_ID, 2 + val)));
        range(0, 20).forEach(val -> eventBus.send(ADDR_REPLAY_UPDATES_BASE + DEFAULT_GAME_ID, new JsonObject().put("up-id", val)));
        eventBus.<JsonObject>consumer(ADDR_BROWSER_SPECTATOR_BASE + 1, update -> {
            JsonObject body = update.body();
            if (body.containsKey("sn-id"))
                ctx.assertEquals(4, body.getInteger("sn-id"));
            if (body.containsKey("up-id")) {
                ctx.assertEquals(7, body.getInteger("up-id"));
                async.complete();
            }
        });
        //wait a little to let the events arrive
        rule.vertx().setTimer(200, timeout -> {
            //register
            eventBus.<Integer>send(ADDR_REPLAY_REGISTER_BASE + DEFAULT_GAME_ID, new JsonObject().put(Constants.REPLAY_INDEX, 4), response -> {
                //start streaming
                eventBus.send(ADDR_REPLAY_START_BASE + DEFAULT_GAME_ID, new JsonObject().put(Constants.SPECTATOR_ID, response.result().body()));
            });
        });
    }

}
