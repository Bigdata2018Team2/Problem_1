// TupleComparator.java

package common;

import scala.Tuple2;

import java.io.Serializable;

public class TupleComparator implements org.apache.spark.api.java.function.Function<Tuple2<Integer, Integer>, Long>, Serializable {
    @Override
    public Long call(Tuple2<Integer, Integer> t) throws Exception {
        return t._1() * 10000L + t._2();
    }
}
