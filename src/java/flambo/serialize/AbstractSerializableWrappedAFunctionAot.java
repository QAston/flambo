package flambo.serialize;

import clojure.lang.AFunction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static flambo.serialize.Utils.readAotFunction;
import static flambo.serialize.Utils.writeAotFunction;

/**
 * Created by cbetz on 03.12.14.
 */
public abstract class AbstractSerializableWrappedAFunctionAot implements Serializable {
    protected AFunction f;

    public AbstractSerializableWrappedAFunctionAot() {
    }

    public AbstractSerializableWrappedAFunctionAot(AFunction func) {
        f = func;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeAotFunction(out, f);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        f = readAotFunction(in);
    }


}
