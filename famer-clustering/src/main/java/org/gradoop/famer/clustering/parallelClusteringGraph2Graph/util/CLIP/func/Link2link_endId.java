/*
 * Copyright © 2016 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gradoop.famer.clustering.parallelClusteringGraph2Graph.util.CLIP.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class Link2link_endId implements MapFunction <Edge, Tuple2<Edge, String>>{
    private Integer endType;
    public Link2link_endId(Integer inputEndType){endType = inputEndType;}
    @Override
    public Tuple2<Edge, String> map(Edge edge) throws Exception {
        if (endType == 0)
            return Tuple2.of(edge, edge.getSourceId().toString());
        else
            return Tuple2.of(edge, edge.getTargetId().toString());
    }
}
