package flambo.kryo;

import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.AFunction;

public class Utils {

    //static final Var require = RT.var("clojure.core", "require");
    //static final Var symbol = RT.var("clojure.core", "symbol");

    static final IFn require = Clojure.var("clojure.core", "require");

    private Utils() {
    }

    public static void requireNamespace(String namespace) {

        try {
            require.invoke(Clojure.read(namespace));
        } catch (Exception e) {
            System.out.println("WARN: Failed loading ns:" + namespace);
        }
    }

    public static void writeAFunction(ObjectOutputStream out, AFunction f) throws IOException {
        out.writeObject(f.getClass().getName());
        out.writeObject(f);
    }

    public static AFunction readAFunction(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String clazz = (String) in.readObject();
        String namespace = clazz.split("\\$")[0];

        requireNamespace(namespace);

        return (AFunction) in.readObject();
    }
}
