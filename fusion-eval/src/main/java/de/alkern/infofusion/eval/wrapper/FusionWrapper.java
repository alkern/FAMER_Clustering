package de.alkern.infofusion.eval.wrapper;

import de.alkern.infofusion.eval.util.GraphIO;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.gretl.graph.operations.info_fusion.GraphToGraphVertexFusion;

public class FusionWrapper {

    private final GradoopFlinkConfig config;

    public FusionWrapper(GradoopFlinkConfig config) {
        this.config = config;
    }

    /**
     * Execute the given query on the MusicBrainz0.45 graph
     * @param query to execute
     * @return the fused result graph
     */
    public LogicalGraph fuse(String query) {
        LogicalGraph input = GraphIO.getPreparedMusicbrainzGraph(config);
        GraphToGraphVertexFusion operator = new GraphToGraphVertexFusion(query);
        return input.callForGraph(operator);
    }
}
