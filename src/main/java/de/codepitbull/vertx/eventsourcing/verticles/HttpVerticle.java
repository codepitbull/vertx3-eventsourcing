package de.codepitbull.vertx.eventsourcing.verticles;

import de.codepitbull.vertx.eventsourcing.constants.Addresses;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.BodyHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import io.vertx.rxjava.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.rxjava.ext.web.templ.HandlebarsTemplateEngine;
import io.vertx.rxjava.ext.web.templ.TemplateEngine;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.*;
import static de.codepitbull.vertx.eventsourcing.constants.Addresses.GAMES_CREATE;
import static de.codepitbull.vertx.eventsourcing.constants.Addresses.GAMES_GET_ONE;
import static de.codepitbull.vertx.eventsourcing.constants.Addresses.GAME_BASE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.vertx.core.http.HttpHeaders.LOCATION;
import static io.vertx.core.http.HttpHeaders.TEXT_HTML;

/**
 * This verticle provides the REST-API for the game, including the eventbus-bridge.
 *
 * @author Jochen Mader
 */
public class HttpVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(HttpVerticle.class);

    public static final String FORM_NR_OF_PLAYERS = "nr_of_players";
    public static final String FORM_PLAYER_NAME = "player_name";
    public static final String FORM_SPECTATOR_INDEX = "index";
    public static final String URL_GAMEID = "gameid";
    public static final String URL_PLAYERID = "playerid";
    public static final String URL_SPECTATORID = "spectatorid";

    private TemplateEngine engine = HandlebarsTemplateEngine.create();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Router router = Router.router(vertx);

        initEventbus(router);

        router.route().handler(BodyHandler.create());
        router.route("/static/*").handler(StaticHandler.create().setFilesReadOnly(false).setCacheEntryTimeout(1).setMaxAgeSeconds(2));

        String text_html = TEXT_HTML.toString();

        router.get("/").produces(text_html).handler(this::getGames);

        router.get("/api/games/:gameid/spectators").produces(text_html).handler(this::getSpectators);
        router.post("/api/games/:gameid/spectators").produces(text_html).handler(this::createSpectator);
        router.get("/api/games/:gameid/spectators/:spectatorid").produces(text_html).handler(this::joinGameAsSpectator);

        router.get("/api/games/:gameid/players").produces(text_html).handler(this::getPlayers);
        router.post("/api/games/:gameid/players").produces(text_html).handler(this::createPlayer);
        router.get("/api/games/:gameid/players/:playerid").produces(text_html).handler(this::joinGameAsPlayer);

        router.get("/api/games").produces(text_html).handler(this::getGames);
        router.post("/api/games").produces(text_html).handler(this::createGame);
        router.get("/api/games/:gameid").produces(text_html).handler(this::getGame);
        router.delete("/api/games/:gameid").produces(text_html).handler(this::deleteGame);


        vertx.createHttpServer().requestHandler(router::accept).listenObservable(config().getInteger(CONFIG_PORT, 8070))
                .subscribe(
                        success -> {
                            LOG.info("Succeeded deploying " + HttpVerticle.class);
                            startFuture.complete();
                        },
                        failure -> {
                            LOG.info("Failed deploying " + HttpVerticle.class, failure);
                            startFuture.fail(failure);
                        }
                );
    }

    private void initEventbus(Router router) {
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
        BridgeOptions options = new BridgeOptions()
                .addOutboundPermitted(new PermittedOptions().setAddressRegex("browser.game..*"))
                .addOutboundPermitted(new PermittedOptions().setAddressRegex("browser.replay..*"))
                .addInboundPermitted(new PermittedOptions().setAddressRegex("game..*"))
                .addInboundPermitted(new PermittedOptions().setAddressRegex("replay..*"));
        sockJSHandler.bridge(options);
        router.route("/eventbus/*").handler(sockJSHandler);
    }

    public void getPlayers(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        ctx.put(URL_GAMEID, gameId);
        vertx.eventBus().send(GAMES_GET_ONE, gameId,
                res -> {
                    if (res.succeeded())
                        engine.render(ctx, "templates/player.hbs", result -> ctx.response().end(result.result()));
                    else
                        ctx.response().setStatusCode(NOT_FOUND.code());
                }
        );
    }

    public void createPlayer(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        ctx.put(URL_GAMEID, gameId);
        JsonObject req = new JsonObject()
                .put(ACTION, ACTION_REG)
                .put(PLAYER_NAME, ctx.request().formAttributes().get(FORM_PLAYER_NAME));

        vertx.eventBus().send(GAME_BASE + gameId, req, res -> {
            if (res.failed())
                ctx.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end(res.cause().toString());
            else
                ctx.response().setStatusCode(MOVED_PERMANENTLY.code()).putHeader(LOCATION.toString(), "/api/games/" + gameId + "/players/" + res.result().body()).end();
        });
    }

    public void getGames(RoutingContext ctx) {
        engine.render(ctx, "templates/creategame.hbs", result -> ctx.response().end(result.result()));
    }

    public void createGame(RoutingContext ctx) {
        Integer nrOfPlayers = Integer.valueOf(ctx.request().formAttributes().get(FORM_NR_OF_PLAYERS));
        vertx.eventBus().<Integer>send(GAMES_CREATE, nrOfPlayers, resp -> {
            if (resp.succeeded())
                ctx.response().setStatusCode(MOVED_PERMANENTLY.code()).putHeader(LOCATION.toString(), "/api/games/" + resp.result().body()).end();
            else
                ctx.response().setStatusCode(INTERNAL_SERVER_ERROR.code()).end();
        });
    }

    public void joinGameAsPlayer(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        Integer playerid = Integer.parseInt(ctx.request().getParam(URL_PLAYERID));
        ctx.put(URL_GAMEID, gameId);
        ctx.put(URL_PLAYERID, playerid);
        vertx.eventBus().send(GAMES_GET_ONE, gameId,
                res -> {
                    if (res.succeeded())
                        engine.render(ctx, "templates/playgame.hbs", result -> ctx.response().end(result.result()));
                    else
                        ctx.response().setStatusCode(NOT_FOUND.code());
                }
        );
    }

    public void deleteGame(RoutingContext ctx) {
        ctx.put(URL_GAMEID, Integer.parseInt(ctx.request().getParam(URL_GAMEID)));
        engine.render(ctx, "templates/playgame.hbs", result -> ctx.response().end(result.result()));
    }

    public void getGame(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        ctx.put(URL_GAMEID, gameId);
        vertx.eventBus().send(GAMES_GET_ONE, gameId,
                res -> {
                    if (res.succeeded())
                        engine.render(ctx, "templates/game.hbs", result -> ctx.response().end(result.result()));
                    else
                        ctx.response().setStatusCode(NOT_FOUND.code());
                }
        );
    }

    public void getSpectators(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        ctx.put(URL_GAMEID, gameId);
        vertx.eventBus().send(GAMES_GET_ONE, gameId,
                res -> {
                    if (res.succeeded())
                        engine.render(ctx, "templates/spectator.hbs", result -> ctx.response().end(result.result()));
                    else
                        ctx.response().setStatusCode(NOT_FOUND.code());
                }
        );
    }

    public void createSpectator(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        ctx.put(URL_GAMEID, gameId);
        JsonObject req = new JsonObject()
                .put(REPLAY_INDEX, Integer.parseInt(ctx.request().formAttributes().get(FORM_SPECTATOR_INDEX)));

        vertx.eventBus().send(Addresses.REPLAY_REGISTER_BASE + gameId, req, res -> {
            if (res.failed())
                ctx.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).end(res.cause().toString());
            else
                ctx.response().setStatusCode(MOVED_PERMANENTLY.code()).putHeader(LOCATION.toString(), "/api/games/" + gameId + "/spectators/" + res.result().body()).end();
        });
    }

    public void joinGameAsSpectator(RoutingContext ctx) {
        String gameId = ctx.request().getParam(URL_GAMEID);
        Integer spectatorid = Integer.parseInt(ctx.request().getParam(URL_SPECTATORID));
        ctx.put(URL_GAMEID, gameId);
        ctx.put(URL_SPECTATORID, spectatorid);
        vertx.eventBus().<Integer>send(GAMES_GET_ONE, gameId,
                res -> {
                    if (res.succeeded()) {
                        engine.render(ctx, "templates/watchgame.hbs", result -> ctx.response().end(result.result()));
                    } else
                        ctx.response().setStatusCode(NOT_FOUND.code());
                }
        );
    }
}
