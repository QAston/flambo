package flambo.aotfna;

import clojure.lang.AFunction;
import flambo.serialize.*;
import org.apache.spark.api.java.function.VoidFunction;

public class FlamboVoidFunction extends AbstractSerializableWrappedAFunctionAot implements VoidFunction{
  
  @SuppressWarnings("unchecked")
  public void call(Object v1) throws Exception {
    f.invoke(v1);
  }

    public FlamboVoidFunction(AFunction func) {
        super(func);
    }
}
