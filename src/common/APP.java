// APP.java

package common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

public class APP {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Pair Matching");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> data = jsc.textFile(args[0]);
        double THRESHOLD = Double.parseDouble(args[1]);
        System.out.println("Start Calculating with threshold " + THRESHOLD + "\nFile: " + args[0]);

        JavaPairRDD<Integer, Integer> pairs = data.mapToPair(l -> {
            String[] f = l.split("\t");
            return new Tuple2<>(Integer.parseInt(f[0]), Integer.parseInt(f[1]));
        });
        JavaPairRDD<Integer, Integer> dPairs = pairs.flatMapToPair(g -> {
            ArrayList<Tuple2<Integer, Integer>> result = new ArrayList<>();

            result.add(new Tuple2<>(g._1(), g._2()));
            result.add(new Tuple2<>(g._2(), g._1()));

            return result.iterator();
        });


        JavaPairRDD<Integer, ArrayList<Integer>> table = dPairs.groupByKey().mapToPair(g -> {
            ArrayList<Integer> friends = new ArrayList<>();
            for (Integer f : g._2())
                friends.add(f);
            return new Tuple2<>(g._1(), friends);
        });

        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, Integer>>> invertedTable = table.flatMapToPair(c -> {
            ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> result = new ArrayList<>();
            Integer id = c._1();
            ArrayList<Integer> fs = c._2();
            int fCount = fs.size();

            for (Integer f : fs) {
                Tuple2<Integer, Integer> val = new Tuple2<>(id, fCount);
                result.add(new Tuple2<>(f, val));
            }
            return result.iterator();
        }).groupByKey().mapToPair(item -> {
            ArrayList<Tuple2<Integer, Integer>> fs = new ArrayList<>();
            for (Tuple2<Integer, Integer> f : item._2())
                fs.add(f);
            return new Tuple2<>(item._1(), fs);
        });

        // Map
        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Integer> allCandidate = invertedTable.flatMapToPair(p -> {
            ArrayList<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Integer>> result = new ArrayList<>();
            int size = p._2().size();
            for (int i = 0; i < size - 1; ++i) {
                for (int j = i + 1; j < size; ++j) {
                    Tuple2<Integer, Integer> key1 = new Tuple2<>(p._2().get(i)._1(), p._2().get(i)._2());
                    Tuple2<Integer, Integer> key2 = new Tuple2<>(p._2().get(j)._1(), p._2().get(i)._2());
                    result.add(new Tuple2<>(new Tuple2<>(key1, key2), 1));
                }
            }
            return result.iterator();
        });

        // Reduce
        JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Integer> overlapCount = allCandidate.reduceByKey((a, b) -> a + b);
        JavaRDD<Tuple2<Integer, Integer>> overlap = overlapCount.filter(g -> {
            int countA = g._1()._1()._2(), countB = g._1()._2()._2();
            double theta = THRESHOLD / (1 + THRESHOLD) * (countA + countB);
            return g._2() >= theta;
        }).mapToPair(g -> g._1()._1()._1() > g._1()._2()._1() ?
                new Tuple2<>(g._1()._2()._1(), g._1()._1()._1()) : new Tuple2<>(g._1()._1()._1(), g._1()._2()._1())
                ).subtractByKey(pairs).map(p -> new Tuple2<>(p._1(), p._2()));
        JavaRDD<Tuple2<Integer, Integer>> sortedOverlap = overlap.sortBy(new TupleComparator(), true, 2);

        for (Tuple2<Integer, Integer> candidate : sortedOverlap.collect()) {
            System.out.println(candidate._1() + "\t" + candidate._2());
        }
    }
}
