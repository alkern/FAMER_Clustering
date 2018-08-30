package de.alkern.infofusion.eval;

import de.alkern.infofusion.eval.util.DataPreparer;
import de.alkern.infofusion.eval.util.FusionQueries;
import de.alkern.infofusion.eval.util.GraphIO;
import de.alkern.infofusion.eval.wrapper.FamerWrapper;
import de.alkern.infofusion.eval.wrapper.FusionWrapper;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.famer.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasures;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;

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
        String query = FusionQueries.MUSICBRAINZ_QUERY;
        LogicalGraph fusionResult = fusionWrapper.fuse(query);

        // Combine source and result into one graph and save it as merged
        LogicalGraph mergedWithSource = dataPreparer.mergeIntoSourceData(fusionResult);
        graphIO.saveMergedGraph(mergedWithSource);

        // Link and cluster the merged graph
        famerWrapper.link(mergedWithSource);
        LogicalGraph clusteredResultGraph = famerWrapper.executeConcomClustering(mergedWithSource);

        //Check if a cluster contains more than one vertex
//        clusteredResultGraph.getVertices()
//                .map(new MapFunction<Vertex, Tuple2<String, Vertex>>() {
//                    @Override
//                    public Tuple2<String, Vertex> map(Vertex vertex) throws Exception {
//                        return new Tuple2<>(vertex.getPropertyValue("ClusterId").getString(), vertex);
//                    }
//                })
//                .groupBy(0)
//                .combineGroup(new GroupCombineFunction<Tuple2<String, Vertex>, Tuple2<Integer, String>>() {
//                    @Override
//                    public void combine(Iterable<Tuple2<String, Vertex>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
//                        int counter = 0;
//                        String clusterId = "";
//                        for (Tuple2<String, Vertex> it : iterable) {
//                            counter++;
//                            clusterId = it.f0;
//                        }
//                        collector.collect(new Tuple2<>(counter, clusterId));
//                    }
//                })
//                .filter(new FilterFunction<Tuple2<Integer, String>>() {
//                    @Override
//                    public boolean filter(Tuple2<Integer, String> integer) throws Exception {
//                        return integer.f0 > 1;
//                    }
//                })
//                .print();
//
//        clusteredResultGraph.getVertices()
//                .filter(new FilterFunction<Vertex>() {
//                    @Override
//                    public boolean filter(Vertex vertex) throws Exception {
//                        return vertex.getPropertyValue("ClusterId") == null;
//                    }
//                })
//                .print();

        graphIO.saveResultGraph(clusteredResultGraph);

        // Analyze
//        Boolean hasOverlap = false;
//
//        FileWriter fw = new FileWriter(graphIO.getResultPath() + "quality.csv", true);
//        fw.append("Pre,Rec,FM\n");
//        ComputeClusteringQualityMeasures eval = new ComputeClusteringQualityMeasures(resultGraph, "ClusterId", hasOverlap);
//        fw.append(eval.computePrecision() + "," + eval.computeRecall() + "," + eval.computeFM() + "\n");
//        fw.flush();

        env.execute();
    }
}
