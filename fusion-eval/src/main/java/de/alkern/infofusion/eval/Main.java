package de.alkern.infofusion.eval;

import de.alkern.infofusion.eval.util.DataPreparer;
import de.alkern.infofusion.eval.util.GraphIO;
import de.alkern.infofusion.eval.wrapper.FamerWrapper;
import de.alkern.infofusion.eval.wrapper.FusionWrapper;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.famer.example.ClusteringExample;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class Main {

    public static void main(String[] args) throws Exception {
        // Setup
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        GraphIO graphIO = new GraphIO(config);
        FusionWrapper fusionWrapper = new FusionWrapper(graphIO);
        FamerWrapper famerWrapper = new FamerWrapper(graphIO);
        DataPreparer dataPreparer = new DataPreparer(config);

        // Run the query
        String query = "" +
                "CREATE MERGING\n" +
                "LET song\n" +
                "  title = property(\n" +
                "    (MusicSource.Song.name,\n" +
                "    Trackz.Track.title,\n" +
                "    MusicDB.Song.title,\n" +
                "    SongDB.Song.song_name,\n" +
                "    SongArchive.Song.song_name),\n" +
                "    textual:lcs)\n";
        LogicalGraph result = fusionWrapper.fuse(query);

        // Combine source and result into one graph and save it as merged
        LogicalGraph mergedWithSource = dataPreparer.mergeIntoSourceData(result);
        graphIO.saveMergedGraph(mergedWithSource, config); //TODO speichern klappt nicht
//        mergedWithSource.writeTo(new JSONDataSink(
//                graphIO.getMergedPath() + "graphHeads.json",
//                graphIO.getMergedPath() + "vertices.json",
//                graphIO.getMergedPath() + "edges.json",
//                config
//        ));

        // Cluster the merged graph
        famerWrapper.executeClusteringExample();
        famerWrapper.executeClusteringExample(ClusteringExample.ClusteringMethods.CONCON,
                graphIO.getMergedPath(), graphIO.getResultPath());

        // Analyze
        env.execute();
    }
}
