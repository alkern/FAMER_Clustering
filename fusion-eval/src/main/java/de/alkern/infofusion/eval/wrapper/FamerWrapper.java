package de.alkern.infofusion.eval.wrapper;

import de.alkern.infofusion.eval.util.GraphIO;
import org.gradoop.famer.example.ClusteringExample;

public class FamerWrapper {

    public void executeClusteringExample() throws Exception {
        executeClusteringExample(ClusteringExample.ClusteringMethods.CLIP);
    }

    public void executeClusteringExample(ClusteringExample.ClusteringMethods method) throws Exception {
        ClusteringExample clustering = new ClusteringExample();
        String srcFolder = GraphIO.getMusicbrainzThreshold045Path();

        String resFolder = FamerWrapper.class.getResource("/musicbrainz/").getFile()
                .replace("musicbrainz", "result")
                .replace("target/classes", "src/main/resources");
        clustering.execute(method, srcFolder, resFolder, 5);
    }
}
