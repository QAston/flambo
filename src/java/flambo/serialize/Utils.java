package flambo.serialize;

import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.AFunction;
import clojure.lang.Var;
import clojure.lang.RT;

public class Utils {

    static final IFn require;
    static final Var symbol;
    static final IFn writeJitToOut;
    static final IFn readJitFromIn;

    static {
        require = Clojure.var("clojure.core", "require");
        symbol = RT.var("clojure.core", "symbol");
        require.invoke(Clojure.read("flambo.function"));
        writeJitToOut = Clojure.var("flambo.function", "write-to-output-stream");
        readJitFromIn = Clojure.var("flambo.function", "read-from-input-stream");
    }

    private Utils() {
    }

    public static void requireNamespace(String namespace) {

        try {
            require.invoke(symbol.invoke(namespace));
        } catch (Exception e) {
            System.out.println("WARN: Failed loading ns:" + namespace);
        }
    }

    public static void writeAotFunction(ObjectOutputStream out, AFunction f) throws IOException {
        out.writeObject(f.getClass().getName());
        out.writeObject(f);
    }

    public static AFunction readAotFunction(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String clazz = (String) in.readObject();
        String namespace = clazz.split("\\$")[0];

        requireNamespace(namespace);

        return (AFunction) in.readObject();
    }

    public static void writeJitFunction(ObjectOutputStream out, Object f) throws IOException {
        writeJitToOut.invoke(f, out);
    }

    public static Object readJitFunction(ObjectInputStream in) throws IOException, ClassNotFoundException {
        return readJitFromIn.invoke(in);
    }
}
