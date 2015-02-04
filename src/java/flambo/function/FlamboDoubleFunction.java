package flambo.function;

import clojure.lang.AFunction;
import flambo.kryo.AbstractSerializableWrappedAFunction;
import org.apache.spark.api.java.function.DoubleFunction;

public class FlamboDoubleFunction extends AbstractSerializableWrappedAFunction implements DoubleFunction {
    public FlamboDoubleFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public double call(Object v1) throws Exception {
    return (Double) f.invoke(v1);
  }
}
