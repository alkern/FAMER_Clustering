package de.alkern.infofusion.eval.wrapper;

import de.alkern.infofusion.eval.util.GraphIO;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.CLIP;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.ConnectedComponents;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.CLIPConfig;
import org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.ClusteringOutputType;
import org.gradoop.famer.example.ClusteringExample;
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
        return input.callForGraph(new ConnectedComponents());
    }

    public LogicalGraph executeClipClustering(LogicalGraph input) {
        CLIPConfig clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(6);
        ClusteringOutputType clusteringOutputType = ClusteringOutputType.GRAPH;
        return input.callForGraph(new CLIP(clipConfig, clusteringOutputType));
    }
}
