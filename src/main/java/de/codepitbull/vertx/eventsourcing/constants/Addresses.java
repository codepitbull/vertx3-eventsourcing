package de.codepitbull.vertx.eventsourcing.constants;

/**
 * EventBus Addresses used in this application.
 */
public class Addresses {

    public static final String REPLAY_REGISTER_BASE = "replay.register.";
    public static final String REPLAY_SNAPSHOTS_BASE = "replay.snapshots.";
    public static final String REPLAY_UPDATES_BASE = "replay.updates.";
    public static final String REPLAY_START_BASE = "replay.start.";
    public static final String BROWSER_SPECTATOR_BASE = "browser.replay.";
    public static final String GAMES_CREATE = "games.create";
    public static final String GAMES_GET_ONE = "games.get";
    public static final String GAMES_DELETE = "games.delete";
    public static final String GAMES_LIST = "games.list";
    public static final String GAME_BASE = "game.";
    public static final String BROWSER_GAME_BASE = "browser.game.";

    private Addresses() {};
}
