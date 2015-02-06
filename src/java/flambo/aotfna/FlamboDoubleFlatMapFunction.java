package flambo.aotfna;

import clojure.lang.AFunction;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import flambo.serialize.*;

public class FlamboDoubleFlatMapFunction extends AbstractSerializableWrappedAFunctionAot implements DoubleFlatMapFunction {
    public FlamboDoubleFlatMapFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public Iterable<Double> call(Object v1) throws Exception {
    return (Iterable<Double>) f.invoke(v1);
  }
}
