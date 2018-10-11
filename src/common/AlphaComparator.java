package common;

import org.apache.spark.api.java.function.Function;
import scala.Serializable;
import scala.Tuple2;

public class AlphaComparator implements Function<Tuple2<Tuple2<Integer, Integer>, Double>, Double>, Serializable {
    @Override
    public Double call(Tuple2<Tuple2<Integer, Integer>, Double> t) throws Exception {
        return t._2();
    }
}