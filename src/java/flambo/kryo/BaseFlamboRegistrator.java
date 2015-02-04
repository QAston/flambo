package flambo.kryo;

import clojure.lang.IFn;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import carbonite.JavaBridge;
import org.objenesis.strategy.StdInstantiatorStrategy;
import scala.Tuple2;
import com.twitter.chill.Tuple2Serializer;
import clojure.java.api.Clojure;

public class BaseFlamboRegistrator implements KryoRegistrator {

    static {
        Utils.requireNamespace("flambo.kryo.sfn-serializer");
    }

    protected void register(Kryo kryo) {
    }

    @Override
    public final void registerClasses(Kryo kryo) {
        try {
            JavaBridge.enhanceRegistry(kryo);
            kryo.register(Tuple2.class, new Tuple2Serializer());
            IFn register = Clojure.var("flambo.kryo.sfn-serializer", "register");
            register.invoke(kryo);
            //kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
            register(kryo);

      /*
        We do this because under mesos these serializers don't get registered
        in the executors like they should and do in the driver which leads to
        kryo class ID mismatches. Forcing the registration here works around the
        problem.
      */

            // kryo.register(scala.collection.convert.Wrappers.IteratorWrapper.class);
            // kryo.register(scala.collection.convert.Wrappers.SeqWrapper.class);
            // kryo.register(scala.collection.convert.Wrappers.MapWrapper.class);
            // kryo.register(scala.collection.convert.Wrappers.JListWrapper.class);
            // kryo.register(scala.collection.convert.Wrappers.JMapWrapper.class);

        } catch (Exception e) {
            throw new RuntimeException("Failed to register kryo!", e);
        }
    }
}
