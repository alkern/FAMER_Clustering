package de.alkern.infofusion.eval;

import de.alkern.infofusion.eval.util.GraphGenerator;
import org.gradoop.famer.example.ClusteringExample;

public class FamerWrapper {

    public static void main(String[] args) throws Exception {
//        String query = "" +
//                "CREATE MERGING\n" +
//                "LET test\n" +
//                "  value = majority(\n" +
//                "    (source1.Label.value,\n" +
//                "    source2.Label2.value,\n" +
//                "    source3.Label3.v))\n";
//        GraphToGraphVertexFusion op = new GraphToGraphVertexFusion(query);
//        LogicalGraph graph = GraphGenerator.createTestGraph();
//        System.out.println("INPUT");
//        graph.getVertices()
//                .collect().forEach(System.out::println);
//        System.out.println("RESULT");
//        graph.callForGraph(op).getVertices()
//                .collect().forEach(System.out::println);

        FamerWrapper wrapper = new FamerWrapper();
        wrapper.executeClusteringExample();
        System.out.println("Done!");
    }

    public void executeClusteringExample() throws Exception {
        ClusteringExample clustering = new ClusteringExample();
        String srcFolder = GraphGenerator.getMusicbrainzThreshold045();
        String resFolder = FamerWrapper.class.getResource("/musicbrainz/").getFile()
                .replace("musicbrainz", "result")
                .replace("target/classes", "src/main/resources");
        clustering.execute(ClusteringExample.ClusteringMethods.CLIP, srcFolder, resFolder, 5);
    }
}
