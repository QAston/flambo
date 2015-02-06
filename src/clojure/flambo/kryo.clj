;; ## Utilities and macros for dealing with kryo
;;
(ns flambo.kryo
  (:import [org.apache.spark.serializer KryoRegistrator]
           [org.apache.spark SparkEnv]
           [org.apache.spark.serializer SerializerInstance]
           [java.nio ByteBuffer]
           [scala.reflect ClassTag$]
           (scala Tuple2)
           (org.apache.spark.api.java JavaSparkContext$)))

;; lol scala
(def ^:no-doc OBJECT-CLASS-TAG (.apply ClassTag$/MODULE$ java.lang.Object))

(def ^:no-doc DOUBLE-CLASS-TAG (.apply ClassTag$/MODULE$ java.lang.Double))

(def ^:no-doc TUPLE-CLASS-TAG (.apply ClassTag$/MODULE$ Tuple2))

(def ^:no-doc FAKE-CLASS-TAG (.fakeClassTag JavaSparkContext$/MODULE$))

(defn ^bytes serialize
  "We piggy back off of spark's kryo instance from `SparkEnv` since it already
  has all of our custom serializers and other things we need to serialize our functions."
  [^Object obj]
  (let [^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)
        ^ByteBuffer buf (.serialize ser obj OBJECT-CLASS-TAG)]
    (.array buf)))

(defn deserialize
  "We piggy back off of spark's kryo instance from `SparkEnv` for the same
  reasons we do so in `serialize`."
  [^bytes b]
  (let [^ByteBuffer buf (ByteBuffer/wrap b)
        ^SerializerInstance ser (.. (SparkEnv/get) serializer newInstance)]
    (.deserialize ser buf OBJECT-CLASS-TAG)))

(defn ^bytes serialize-closure
  "We piggy back off of spark's kryo instance from `SparkEnv` since it already
  has all of our custom serializers and other things we need to serialize our functions."
  [^Object obj]
  (let [^SerializerInstance ser (.. (SparkEnv/get) closureSerializer newInstance)
        ^ByteBuffer buf (.serialize ser obj OBJECT-CLASS-TAG)]
    (.array buf)))

(defn deserialize-closure
  "We piggy back off of spark's kryo instance from `SparkEnv` for the same
  reasons we do so in `serialize`."
  [^bytes b]
  (let [^ByteBuffer buf (ByteBuffer/wrap b)
        ^SerializerInstance ser (.. (SparkEnv/get) closureSerializer newInstance)]
    (.deserialize ser buf OBJECT-CLASS-TAG)))

(defmacro defregistrator
  "A macro for creating a custom kryo registrator for application that need to
  register custom kryo serializers.

  This macro must be called from a namespace that is AOT compiled. This is not typically
  an issue since application jars are packaged as uberjars.

  Note that we are extending `BaseFlamboRegistrator` and not spark's `KryoRegistrator`."
  [name & register-impl]
  (let [prefix (gensym)
        classname (str *ns* ".registrator." name)]
    `(do
       (gen-class :name ~classname
                  :extends flambo.kryo.BaseFlamboRegistrator
                  :prefix ~prefix)
       (defn ~(symbol (str prefix "register"))
         ~@register-impl)
       (def ~name ~classname))))
