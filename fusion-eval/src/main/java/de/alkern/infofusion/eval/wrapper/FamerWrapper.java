package de.alkern.infofusion.eval.wrapper;

import de.alkern.infofusion.eval.util.GraphIO;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.CLIP;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.ConnectedComponents;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.CLIPConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.example.ClusteringExample;
import org.gradoop.famer.graphGenerator.Blocking.BlockingComponent2;
import org.gradoop.flink.model.api.epgm.LogicalGraph;

public class FamerWrapper {

    private final ClusteringExample clustering;
    private final GraphIO graphIO;

    public FamerWrapper(GraphIO graphIO) {
        this.graphIO = graphIO;
        this.clustering = new ClusteringExample();
    }

    public void executeClusteringExample() throws Exception {
        executeClusteringExample(ClusteringExample.ClusteringMethods.CLIP);
    }

    public void executeClusteringExample(ClusteringExample.ClusteringMethods method) throws Exception {
        String srcFolder = graphIO.getMusicbrainzThreshold045Path();
        String resFolder = graphIO.getResultPath();
        clustering.execute(method, srcFolder, resFolder, 5);
    }

    public void executeClusteringExample(ClusteringExample.ClusteringMethods method, String srcFolder, String resFolder) throws Exception {
        executeClusteringExample(method, srcFolder, resFolder, 6);
    }

    public void executeClusteringExample(ClusteringExample.ClusteringMethods method, String srcFolder, String resFolder, int srcNo) throws Exception {
        clustering.execute(method, srcFolder, resFolder, srcNo);
    }

    public LogicalGraph executeConcomClustering(LogicalGraph input) {
        System.out.println("Start Concom Clustering");
        return input.callForGraph(new ConnectedComponents());
    }

    public LogicalGraph executeClipClustering(LogicalGraph input) {
        CLIPConfig clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(6);
        ClusteringOutputType clusteringOutputType = ClusteringOutputType.GRAPH;
        return input.callForGraph(new CLIP(clipConfig, clusteringOutputType));
    }

    public void link(LogicalGraph graph) throws Exception {
        System.out.println("Start Linking");
        BlockingComponent2 blocking = new BlockingComponent2("2", false);
        LogicalGraph graphWithKeyAttribute = graphIO.getLogicalGraphFactory().fromDataSets(
                graph.getVertices()
                .map(new MapFunction<Vertex, Vertex>() {
                    @Override
                    public Vertex map(Vertex vertex) throws Exception {
                        String value = vertex.getId().toString();
                        vertex.setProperty("key", value);
                        return vertex;
                    }
                })
        );
        DataSet<Tuple2<Vertex, Vertex>> blockingResult = blocking.execute(graphWithKeyAttribute);
        blockingResult
//                .filter(new FilterFunction<Tuple2<Vertex, Vertex>>() {
//                    @Override
//                    public boolean filter(Tuple2<Vertex, Vertex> vertexVertexTuple2) throws Exception {
//                        return true;//vertexVertexTuple2.f0.getId().toString().equals("000000000000000000019302");
//                    }
//                })
                .print();
    }
}
