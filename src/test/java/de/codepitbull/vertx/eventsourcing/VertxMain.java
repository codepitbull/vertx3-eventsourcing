package de.codepitbull.vertx.eventsourcing;

import de.codepitbull.vertx.eventsourcing.verticles.GameControlVerticle;
import de.codepitbull.vertx.eventsourcing.verticles.HttpVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Properties;

import static de.codepitbull.vertx.eventsourcing.constants.Constants.CONFIG_PORT;

/**
 * A runner to start the server for testing
 *
 * @author Jochen Mader
 */
public class VertxMain {

    public static void main(String[] args) {
        Properties properties = System.getProperties();
        properties.setProperty("vertx.disableFileCaching", "true");
        properties.setProperty("vertx.cwd","/Users/jmader/Development/2_github-codepitbull/phaer-test/src/main/resources/");
        Vertx vertx = Vertx.vertx();

        vertx.deployVerticle(GameControlVerticle.class.getName(), new DeploymentOptions().setConfig(new JsonObject().put(CONFIG_PORT, 8070)));
        vertx.deployVerticle(HttpVerticle.class.getName(), new DeploymentOptions().setConfig(new JsonObject().put(CONFIG_PORT, 8070)));

    }
}
