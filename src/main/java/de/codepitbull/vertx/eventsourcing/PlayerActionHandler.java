package de.codepitbull.vertx.eventsourcing;

import de.codepitbull.vertx.eventsourcing.entity.Player;

@FunctionalInterface
public interface PlayerActionHandler {
    void action(Player player);
}
