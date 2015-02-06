package flambo.aotfna;

import clojure.lang.AFunction;
import org.apache.spark.api.java.function.DoubleFunction;
import flambo.serialize.*;

public class FlamboDoubleFunction extends AbstractSerializableWrappedAFunctionAot implements DoubleFunction {
    public FlamboDoubleFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public double call(Object v1) throws Exception {
    return (Double) f.invoke(v1);
  }
}
