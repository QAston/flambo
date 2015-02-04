package flambo.function;

import clojure.lang.AFunction;
import flambo.kryo.AbstractSerializableWrappedAFunction;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;

public class FlamboDoubleFlatMapFunction extends AbstractSerializableWrappedAFunction implements DoubleFlatMapFunction {
    public FlamboDoubleFlatMapFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public Iterable<Double> call(Object v1) throws Exception {
    return (Iterable<Double>) f.invoke(v1);
  }
}
