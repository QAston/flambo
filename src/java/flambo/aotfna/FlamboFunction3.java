package flambo.aotfna;

import clojure.lang.AFunction;
import org.apache.spark.api.java.function.Function3;
import flambo.serialize.*;

public class FlamboFunction3 extends AbstractSerializableWrappedAFunctionAot implements Function3 {
    public FlamboFunction3(AFunction func) {
        super(func);
    }

    public Object call(Object v1, Object v2, Object v3) throws Exception {
    return f.invoke(v1, v2, v3);
  }
}
