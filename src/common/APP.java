package common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
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

        JavaPairRDD<Integer, Integer> countTable = table.mapToPair(p -> new Tuple2<>(p._1(), p._2().size()));
        countTable.persist(StorageLevel.DISK_ONLY());
//        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> pairCountTable = countTable.cartesian(countTable)
//                .filter(p -> p._1()._1() < p._2()._2())
//                .mapToPair(p -> new Tuple2<>(new Tuple2<>(p._1()._1(), p._2()._1()), new Tuple2<>(p._1()._2(), p._2()._2())));

        // Map
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> allCandidate = table.flatMapToPair(p -> {
            ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> result = new ArrayList<>();
            int size = p._2().size();
            for (int i = 0; i < size - 1; ++i) {
                for (int j = i + 1; j < size; ++j) {
                    Integer a1 = p._2().get(i), a2 = p._2().get(j);
                    result.add(a1 > a2
                            ? new Tuple2<>(new Tuple2<>(a2, a1), 1)
                            : new Tuple2<>(new Tuple2<>(a1, a2), 1)
                    );
                }
            }
            return result.iterator();
        });

        // Reduce
        JavaPairRDD<Tuple2<Integer, Integer>, Double> overlap = allCandidate.reduceByKey((a, b) -> a + b)
//                .join(pairCountTable)
                .flatMapToPair(p -> {
                    ArrayList<Tuple2<Tuple2<Integer, Integer>, Double>> result = new ArrayList<>();
                    Tuple2<Integer, Integer> keyPair = p._1();
                    int countA = countTable.lookup(p._1()._1()).get(0), countB = countTable.lookup(p._1()._2()).get(0);
                    int common = p._2();

                    double union = 0.0F + countA + countB - common;
                    if (union * THRESHOLD <= common) {
                        result.add(new Tuple2<>(new Tuple2<>(keyPair._1(), keyPair._2()), common / union));
                    }
                    return result.iterator();
                }).subtractByKey(isFriend);//.map(p -> new Tuple2<>(p._1(), p._2()));
        //JavaRDD<Tuple2<Tuple2<Integer, Integer>, Double>> sortedOverlap = overlap.sortBy(new AlphaComparator(), true, 4);

        for (Tuple2<Tuple2<Integer, Integer>, Double> candidate : overlap.collect()) {
            System.out.println(candidate._1()._1() + "\t" + candidate._1()._2());
        }
    }
}
