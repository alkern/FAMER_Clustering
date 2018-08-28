package de.alkern.infofusion.eval.util;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.epgm.LogicalGraphFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.gretl.graph.GraphIoUtils;

import java.io.File;
import java.io.IOException;

public class GraphIO {

    public static LogicalGraph loadGraph(String srcFolder, GradoopFlinkConfig config) {
        JSONDataSource dataSource = new JSONDataSource(srcFolder + "graphHeads.json", srcFolder + "vertices.json", srcFolder + "edges.json", config);
        return dataSource.getLogicalGraph();
    }

    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    public static String getMusicbrainzThreshold045Path() {
        return "F:\\Daten\\Workspaces\\famer_fork\\inputGraphs\\DS2-MusicBrainz\\threshold_0.45\\";
    }

    public static String getResultPath() {
        return getPreparedMusicbrainzPath()
                .replace("musicbrainz", "result")
                .replace("target/classes", "src/main/resources");
    }

    public static String getPreparedMusicbrainzPath() {
        return GraphIO.class.getResource("/musicbrainz/").getFile();
    }

    public static LogicalGraph getPreparedMusicbrainzGraph(GradoopFlinkConfig config) {
        return loadGraph(getPreparedMusicbrainzPath(), config);
    }

    public static LogicalGraph loadMusicbrainzThreshold045(GradoopFlinkConfig config) {
        String srcFolder = getMusicbrainzThreshold045Path();
        return loadGraph(srcFolder, config);
    }

    public static void saveResultGraph(LogicalGraph graph) throws IOException {
        String path = getResultPath();
        FileUtils.deleteDirectory(new File(path));
        GraphIoUtils.writeGraph(graph, path);
    }

    public static LogicalGraph createTestGraph() {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        VertexFactory vertexFactory = config.getVertexFactory();

        Properties properties1 = Properties.create();
        properties1.set("ClusterId", 1);
        properties1.set("type", "source1");
        properties1.set("value", "v1");
        Vertex vertex1 = vertexFactory.createVertex("TestVertex", properties1);
        vertex1.setLabel("Label");

        Properties properties2 = Properties.create();
        properties2.set("ClusterId", 1);
        properties2.set("type", "source2");
        properties2.set("value", "v2");
        Vertex vertex2 = vertexFactory.createVertex("TestVertex", properties2);
        vertex2.setLabel("Label2");

        Properties properties3 = Properties.create();
        properties3.set("ClusterId", 1);
        properties3.set("type", "source3");
        properties3.set("v", "v2");
        Vertex vertex3 = vertexFactory.createVertex("TestVertex", properties3);
        vertex3.setLabel("Label3");

        LogicalGraphFactory graphFactory = config.getLogicalGraphFactory();
        return graphFactory.fromDataSets(env.fromElements(vertex1, vertex2, vertex3));
    }
}