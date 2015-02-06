package flambo.aotfna;

import clojure.lang.AFunction;
import org.apache.spark.api.java.function.Function;
import flambo.serialize.*;

public class FlamboFunction extends AbstractSerializableWrappedAFunctionAot implements Function {
    public FlamboFunction(AFunction func) {
        super(func);
    }

    public Object call(Object v1) throws Exception {
        return f.invoke(v1);
    }
}
