package de.codepitbull.vertx.eventsourcing.maps;

import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

/**
 * Created by jmader on 01.08.15.
 */
public class MapReaderTest {
    @Test
    public void testToString() throws Exception{
        MapReader reader = new MapReader(new JsonObject(IOUtils.toString(MapReader.class.getResourceAsStream("/webroot/maps/map_1.json"))));
        System.out.println(reader);
    }
}
