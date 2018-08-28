package de.alkern.infofusion.eval;

import de.alkern.infofusion.eval.util.GraphIO;
import de.alkern.infofusion.eval.wrapper.FamerWrapper;
import de.alkern.infofusion.eval.wrapper.FusionWrapper;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class Main {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        FusionWrapper fusionWrapper = new FusionWrapper(config);
//        FamerWrapper famerWrapper = new FamerWrapper();

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
        result.getVertices()
                .collect()
                .forEach(System.out::println);
        GraphIO.saveResultGraph(result);
    }
}
