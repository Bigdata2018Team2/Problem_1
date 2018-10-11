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

        // Duplicate the pair to make friends relationship table
        JavaPairRDD<Integer, Integer> dPairs = data.flatMapToPair(l -> {
            ArrayList<Tuple2<Integer, Integer>> result = new ArrayList<>();
            String[] f = l.split("\t");
            int a = Integer.parseInt(f[0]);
            int b = Integer.parseInt(f[1]);

            result.add(new Tuple2<>(a, b));
            result.add(new Tuple2<>(b, a));

            return result.iterator();
        });

        JavaPairRDD<Tuple2<Integer, Integer>, Boolean> isFriend = dPairs.mapToPair(p -> new Tuple2<>(new Tuple2<>(p._1(), p._2()), true));


        // Contains all relationship of each user
        // ex) A -> B, C, D ...
        JavaPairRDD<Integer, ArrayList<Integer>> table = dPairs.groupByKey().mapToPair(g -> {
            ArrayList<Integer> friends = new ArrayList<>();
            for (Integer f : g._2())
                friends.add(f);
            return new Tuple2<>(g._1(), friends);
        });

        // Invert table. And each record in column contain their number of friends
        // ex) A -> (B, 2), (C, 3) ...
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
                    Tuple2<Integer, Integer> a1 = p._2().get(i);
                    Tuple2<Integer, Integer> a2 = p._2().get(j);
                    Tuple2<Integer, Integer> key1 = new Tuple2<>(a1._1(), a1._2());
                    Tuple2<Integer, Integer> key2 = new Tuple2<>(a2._1(), a2._2());
                    result.add(a1._1() > a2._1()
                            ? new Tuple2<>(new Tuple2<>(key2, key1), 1)
                            : new Tuple2<>(new Tuple2<>(key1, key2), 1)
                    );
                }
            }
            return result.iterator();
        });

        // Reduce
        JavaRDD<Tuple2<Tuple2<Integer, Integer>, Double>> overlap = allCandidate.reduceByKey((a, b) -> a + b).flatMapToPair(p -> {
            ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> result = new ArrayList<>();
            int countA = p._1()._1()._2(), countB = p._1()._2()._2();
            double union = 0.0F + countA + countB - p._2();
            if (union * THRESHOLD <= p._2()) {
                result.add(new Tuple2<>(new Tuple2<>(p._1()._1()._1(), p._1()._2()._1()), p._2() / union));
            }
            return result.iterator();
        }).subtractByKey(isFriend).map(p -> new Tuple2<>(p._1(), p._2()));
        //JavaRDD<Tuple2<Tuple2<Integer, Integer>, Double>> sortedOverlap = overlap.sortBy(new AlphaComparator(), true, 4);

        for (Tuple2<Tuple2<Integer, Integer>, Double> candidate : overlap.collect()) {
            System.out.println(candidate._1()._1() + "\t" + candidate._1()._2());
        }
    }
}
