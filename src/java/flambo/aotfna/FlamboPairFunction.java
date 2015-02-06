package flambo.aotfna;

import clojure.lang.AFunction;
import flambo.serialize.*;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class FlamboPairFunction extends AbstractSerializableWrappedAFunctionAot implements PairFunction {
    public FlamboPairFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public Tuple2<Object, Object> call(Object v1) throws Exception {
    return (Tuple2<Object, Object>) f.invoke(v1);
  }
}
