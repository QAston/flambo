package flambo.aotfna;

import clojure.lang.AFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import flambo.serialize.*;

public class FlamboPairFlatMapFunction extends AbstractSerializableWrappedAFunctionAot implements PairFlatMapFunction {
    public FlamboPairFlatMapFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public Iterable<Tuple2<Object, Object>> call(Object v1) throws Exception {
    return (Iterable<Tuple2<Object, Object>>) f.invoke(v1);
  }
}
