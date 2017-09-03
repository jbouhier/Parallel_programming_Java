package edu.coursera.distributed;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A wrapper class for the implementation of a single iteration of the iterative
 * PageRank algorithm.
 */
public final class PageRank {
    /**
     * Default constructor.
     */
    private PageRank() {}

    /**
     *   new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking to B
     *
     *   1) JavaPairRDD.join
     *   2) JavaRDD.flatMapToPair
     *   3) JavaPairRDD.reduceByKey
     *   4) JavaRDD.mapValues
     *
     * @param sites The connectivity of the website graph, keyed on unique website IDs.
     * @param ranks The current ranks of each website, keyed on unique website IDs.
     * @return The new ranks of the websites graph, using the PageRank algorithm to update site ranks.
     */
    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {

        JavaPairRDD<Integer, Double> newRanks = sites
            .join(ranks)
            .flatMapToPair(kv -> {
                Integer websiteId = kv._1();
                Tuple2<Website, Double> value = kv._2();
                Website edges = kv._2()._1();
                Double currentRank = kv._2()._2();

                List<Tuple2<Integer, Double>> contribs = new LinkedList<Tuple2<Integer, Double>>();
                Iterator<Integer> iter = edges.edgeIterator();

                while (iter.hasNext()) {
                    final int target = iter.next();
                    contribs.add(new Tuple2(target, currentRank / (double) edges.getNEdges()));
                }
                return contribs;
            });

        return newRanks.reduceByKey((Double r1, Double r2) -> r1 + r2).mapValues(v -> 0.15 + 0.85 * v);
    }
}
