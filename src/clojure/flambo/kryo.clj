;; ## Utilities and macros for dealing with kryo
;;
(ns flambo.kryo
  (:import [org.apache.spark.serializer KryoRegistrator]
           [org.apache.spark SparkEnv]
           [org.apache.spark.serializer SerializerInstance]
           [java.nio ByteBuffer]
           [scala.reflect ClassTag$]))

;; lol scala
(def ^:no-doc OBJECT-CLASS-TAG (.apply ClassTag$/MODULE$ java.lang.Object))

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