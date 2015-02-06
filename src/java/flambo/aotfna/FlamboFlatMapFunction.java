package flambo.aotfna;

import clojure.lang.AFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import flambo.serialize.*;

public class FlamboFlatMapFunction extends AbstractSerializableWrappedAFunctionAot implements FlatMapFunction {

    public FlamboFlatMapFunction(AFunction func) {
        super(func);
    }

    @SuppressWarnings("unchecked")
  public Iterable<Object> call(Object v1) throws Exception {
    return (Iterable<Object>) f.invoke(v1);
  }
  
}
