package flambo.serialize;

import clojure.lang.AFunction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static flambo.serialize.Utils.readJitFunction;
import static flambo.serialize.Utils.writeJitFunction;

/**
 * Created by cbetz on 03.12.14.
 */
public abstract class AbstractSerializableWrappedAFunctionJit implements Serializable {
    protected Object f;

    public AbstractSerializableWrappedAFunctionJit() {
    }

    public AbstractSerializableWrappedAFunctionJit(Object func) {
        f = func;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeJitFunction(out, f);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        f = readJitFunction(in);
    }


}
