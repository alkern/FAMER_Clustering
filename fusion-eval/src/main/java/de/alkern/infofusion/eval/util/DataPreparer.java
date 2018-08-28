package de.alkern.infofusion.eval.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.gretl.graph.operations.transformation.SetGraphProperty;

/**
 * Transform the Musicbrainz data in a FAMER compatible format
 */
public class DataPreparer {

    private final GradoopFlinkConfig config;
    private final GraphIO graphIO;

    public DataPreparer(GradoopFlinkConfig config) {
        this.config = config;
        this.graphIO = new GraphIO(config);
    }

    public LogicalGraph getMusicbrainzGraph() {
        LogicalGraph unpreparedGraph = graphIO.getPreparedMusicbrainzGraph();
        LogicalGraphFactory factory = config.getLogicalGraphFactory();

        return factory.fromDataSets(unpreparedGraph.getVertices()
                .map(new AddSourceId()));
    }

    public LogicalGraph combineGraphs(LogicalGraph g1, LogicalGraph g2) {
        DataSet<Vertex> vertices1 = g1.getVertices();
        DataSet<Vertex> allVertices = vertices1.union(g2.getVertices());
        return config.getLogicalGraphFactory().fromDataSets(allVertices);
    }

    /**
     * Add srcId 6 to the given graph and merge it with the Musicbrainz graph
     * @param graph
     * @return
     */
    public LogicalGraph mergeIntoSourceData(LogicalGraph graph) {
        Property prop = Property.create("srcId", 6);
        SetGraphProperty operator = new SetGraphProperty(prop);
        return combineGraphs(graph.callForGraph(operator), getMusicbrainzGraph());
    }

    private static class AddSourceId implements MapFunction<Vertex, Vertex> {
        @Override
        public Vertex map(Vertex value) throws Exception {
            String type = value.getPropertyValue("type").getString();
            value.removeProperty("type");
            int sourceId;
            switch (type) {
                case "SongDB":
                    sourceId = 1;
                    break;
                case "Trackz":
                    sourceId = 2;
                    break;
                case "MusicSource":
                    sourceId = 3;
                    break;
                case "SongArchive":
                    sourceId = 4;
                    break;
                case "MusicDB":
                    sourceId = 5;
                    break;
                default:
                    sourceId = 6;
            }
            value.setProperty("srcId", sourceId);
            return value;
        }
    }
}
