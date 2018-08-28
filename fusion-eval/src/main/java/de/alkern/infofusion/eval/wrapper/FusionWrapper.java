package de.alkern.infofusion.eval.wrapper;

import de.alkern.infofusion.eval.util.GraphIO;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.gretl.graph.operations.info_fusion.GraphToGraphVertexFusion;

public class FusionWrapper {

    private final GraphIO graphIO;

    public FusionWrapper(GraphIO graphIO) {
        this.graphIO = graphIO;
    }

    /**
     * Execute the given query on the MusicBrainz0.45 graph
     * @param query to execute
     * @return the fused result graph
     */
    public LogicalGraph fuse(String query) {
        LogicalGraph input = graphIO.getPreparedMusicbrainzGraph();
        GraphToGraphVertexFusion operator = new GraphToGraphVertexFusion(query);
        return input.callForGraph(operator);
    }
}
