;; This is the main entry point to flambo, typically required like `[flambo.api :as f]`.

;; By design, most operations in flambo are built up via the threading macro `->`.

;; The main objective is to provide an idiomatic clojure experience,

;; If you find an RDD operation missing from the api that you'd like to use, pull requests are
;; happily accepted!
;;
(ns flambo.api
  (:refer-clojure :exclude [comp fn reduce count take distinct filter group-by values comparator min max sort-by keys partition-by]
                  :rename {first core-first
                           map   core-map})
  (:require [serializable.fn :as sfn]
            [clojure.tools.logging :as log]
            [flambo.function :refer [flat-map-function
                                     flat-map-function2
                                     function
                                     function2
                                     function3
                                     pair-function
                                     pair-flat-map-function
                                     void-function] :as ff]
            [flambo.conf :as conf]
            [flambo.utils :as u]
            [flambo.kryo :as k]
            [flambo.interop :as fi]
            [flambo.kryo :as kryo])
  (:import [scala Tuple2]
           [java.util Comparator]
           [org.apache.spark.api.java JavaSparkContext StorageLevels
                                      JavaRDD JavaDoubleRDD JavaPairRDD JavaFutureAction JavaRDDLike]
           [org.apache.spark.rdd PartitionwiseSampledRDD]
           (org.apache.spark SparkContext)
           [scala Tuple2]
           [java.util Comparator ArrayList]
           [org.apache.spark.api.java JavaSparkContext StorageLevels
                                      JavaRDD JavaPairRDD JavaDoubleRDD]
           [org.apache.spark HashPartitioner Partitioner]
           [org.apache.spark.rdd PartitionwiseSampledRDD PartitionerAwareUnionRDD]
           [scala.collection JavaConversions]
           [scala.reflect ClassTag$]))

;; flambo makes extensive use of kryo to serialize and deserialize clojure functions
;; and data structures. Here we ensure that these properties are set so they are inhereted
;; into any `SparkConf` objects that are created.
;;
;; flambo WILL NOT WORK without enabling kryo serialization in spark!
;;
(System/setProperty "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
(System/setProperty "spark.kryo.registrator" "flambo.serialize.BaseFlamboRegistrator")

(def STORAGE-LEVELS {:memory-only           StorageLevels/MEMORY_ONLY
                     :memory-only-ser       StorageLevels/MEMORY_ONLY_SER
                     :memory-and-disk       StorageLevels/MEMORY_AND_DISK
                     :memory-and-disk-ser   StorageLevels/MEMORY_AND_DISK_SER
                     :disk-only             StorageLevels/DISK_ONLY
                     :memory-only-2         StorageLevels/MEMORY_ONLY_2
                     :memory-only-ser-2     StorageLevels/MEMORY_ONLY_SER_2
                     :memory-and-disk-2     StorageLevels/MEMORY_AND_DISK_2
                     :memory-and-disk-ser-2 StorageLevels/MEMORY_AND_DISK_SER_2
                     :disk-only-2           StorageLevels/DISK_ONLY_2})

(def storage-memory-only StorageLevels/MEMORY_ONLY)
(def storage-memory-and-disk StorageLevels/MEMORY_AND_DISK)
(def storage-memory-and-disk-ser StorageLevels/MEMORY_AND_DISK_SER)
(def storage-disk-only StorageLevels/DISK_ONLY)
(def storage-memory-only-2 StorageLevels/MEMORY_ONLY_2)
(def storage-memory-only-ser-2 StorageLevels/MEMORY_ONLY_SER_2)
(def storage-memory-and-disk-2 StorageLevels/MEMORY_AND_DISK_2)
(def storage-memory-and-disk-ser-2 StorageLevels/MEMORY_AND_DISK_SER_2)
(def storage-disk-only-2 StorageLevels/DISK_ONLY_2)

(defmacro fn
  [& body]
  `(ff/fn ~@body))

(defmacro defsparkfn [name & fdecl]
  (do
    (if (instance? clojure.lang.Symbol name)
      nil
      (throw (IllegalArgumentException. "First argument to defn must be a symbol")))
    (let [m (if (string? (core-first fdecl))
              {:doc (core-first fdecl)}
              {})
          fdecl (if (string? (core-first fdecl))
                  (next fdecl)
                  fdecl)
          m (if (map? (core-first fdecl))
              (conj m (core-first fdecl))
              m)
          fdecl (if (map? (core-first fdecl))
                  (next fdecl)
                  fdecl)
          fdecl (if (vector? (core-first fdecl))
                  (list fdecl)
                  fdecl)
          m (if (map? (last fdecl))
              (conj m (last fdecl))
              m)
          fdecl (if (map? (last fdecl))
                  (butlast fdecl)
                  fdecl)
          m (let [inline (:inline m)
                  ifn (core-first inline)
                  iname (second inline)]
              ;; same as: (if (and (= 'fn ifn) (not (symbol? iname))) ...)
              (if (if (clojure.lang.Util/equiv 'fn ifn)
                    (if (instance? clojure.lang.Symbol iname) false true))
                ;; inserts the same fn name to the inline fn if it does not have one
                (assoc m :inline (cons ifn (cons (clojure.lang.Symbol/intern (.concat (.getName ^clojure.lang.Symbol name) "__inliner"))
                                                 (next inline))))
                m))
          m (conj (if (meta name) (meta name) {}) m)]
      (list 'def (with-meta name m)
            ;;todo - restore propagation of fn name
            ;;must figure out how to convey primitive hints to self calls first
            (cons `fn fdecl)))))

(defn comparator
  "Returns an implementation of java.util.Comparator based upon pred. Same as clojure.core/comparator"
  {:added  "1.0"
   :static true}
  [pred]
  (fn [x y]
      (cond (pred x y) -1 (pred y x) 1 :else 0)))

;; TODO defsparkfn should accept docstring, keep meta information from defn like location etc.

(defn comp
  "Takes a set of functions and returns a fn that is the composition
  of those fns.  The returned fn takes a variable number of args,
  applies the rightmost of fns to the args, the next
  fn (right-to-left) to the result, etc."
  {:added  "1.0"
   :static true}
  ([] identity)
  ([f] f)
  ([f g]
    (fn
      ([] (f (g)))
      ([x] (f (g x)))
      ([x y] (f (g x y)))
      ([x y z] (f (g x y z)))
      ([x y z & args] (f (apply g x y z args)))))
  ([f g h]
    (fn
      ([] (f (g (h))))
      ([x] (f (g (h x))))
      ([x y] (f (g (h x y))))
      ([x y z] (f (g (h x y z))))
      ([x y z & args] (f (g (apply h x y z args))))))
  ([f g h i]
    (fn
      ([] (f (g (h (i)))))
      ([x] (f (g (h (i x)))))
      ([x y] (f (g (h (i x y)))))
      ([x y z] (f (g (h (i x y z)))))
      ([x y z & args] (f (g (h (apply i x y z args))))))))


(defn spark-context
  "Creates a spark context that loads settings from given configuration object
  or system properties"
  ([conf]
    (log/debug "JavaSparkContext" (conf/to-string conf))
    (JavaSparkContext. conf))
  ([master app-name]
    (log/debug "JavaSparkContext" master app-name)
    (JavaSparkContext. master app-name)))

(defn local-spark-context
  [app-name]
  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name app-name))]
    (spark-context conf)))

(defmacro with-context
  [context-sym conf & body]
  `(let [~context-sym (f/spark-context ~conf)]
     (try
       ~@body
       (finally (.stop ~context-sym)))))

(defn jar-of-ns
  [ns]
  (let [clazz (Class/forName (clojure.string/replace (str ns) #"-" "_"))]
    (JavaSparkContext/jarOfClass clazz)))

(defn- ftruthy?
  [f]
  (fn [x] (u/truthy? (f x))))

;; TODO: accumulators
;; http://spark.apache.org/docs/latest/programming-guide.html#accumulators

;; ## RDD construction
;;
;; Function for constructing new RDDs
;;
(defn text-file
  "Reads a text file from HDFS, a local file system (available on all nodes),
  or any Hadoop-supported file system URI, and returns it as an `JavaRDD` of Strings."
  [spark-context filename]
  (.textFile spark-context filename))

(defn parallelize
  "Distributes a local collection to form/return an RDD"
  ([spark-context lst] (.parallelize spark-context lst))
  ([spark-context lst num-slices] (.parallelize spark-context lst num-slices)))

(defn parallelize-doubles
  "Distributes a local collection to form/return a JavaDoubleRDD"
  ([spark-context lst] (.parallelizeDoubles spark-context (core-map double lst)))
  ([spark-context lst num-slices] (.parallelizeDoubles spark-context (core-map double lst) num-slices)))

(defn parallelize-as-pairs
  "Distributes a local collection to form/return a JavaPairRDD. Tries to convert to scala tuples."
  ([spark-context lst] (.parallelizePairs spark-context (core-map fi/to-tuple lst)))
  ([spark-context lst num-slices] (.parallelizePairs spark-context (core-map fi/to-tuple lst) num-slices)))
  
(defn parallelize-pairs
  "Distributes a local collection to form/return an RDD"
  ([spark-context lst] (.parallelizePairs spark-context lst))
  ([spark-context lst num-slices] (.parallelizePairs spark-context lst num-slices)))

(defn partitioner-aware-union [pair-rdd1 pair-rdd2 & pair-rdds]
  ;; TODO: add check to make sure every rdd is a pair-rdd and has the same partitioner.
  (JavaPairRDD/fromRDD
    (PartitionerAwareUnionRDD.
      (.context pair-rdd1)
      (JavaConversions/asScalaBuffer (into [] (clojure.core/map #(.rdd %1) (conj pair-rdds pair-rdd2 pair-rdd1))))
      (.apply ClassTag$/MODULE$ Object)
      )
    (.apply ClassTag$/MODULE$ Object)
    (.apply ClassTag$/MODULE$ Object)
    ))

(defn partitionwise-sampled-rdd [rdd sampler preserve-partitioning? seed]
  "Creates a PartitionwiseSampledRRD from existing RDD and a sampler object"
  (-> (PartitionwiseSampledRDD.
        (.rdd rdd)
        sampler
        preserve-partitioning?
        seed
        k/OBJECT-CLASS-TAG
        k/OBJECT-CLASS-TAG)
      (JavaRDD/fromRDD k/OBJECT-CLASS-TAG)))

;; ## Transformations
;;
;; Function for transforming RDDs
;;

(defn count-by-entry
  "Return the count of each unique value in `rdd` as a map of (value, count)
  pairs."
  [rdd]
  (into {} (.countByValue rdd)))

(defn key-by
  "Creates tuples of the elements in this RDD by applying `f`."
  [rdd f]
  (.keyBy rdd (function f)))

(defn map
  "Returns a new RDD formed by passing each element of the source through the function `f`."
  [rdd f]
  (.map rdd (function f)))

(defn map-to-pair
  "Returns a new `JavaPairRDD` of (K, V) pairs by applying `f` to all elements of `rdd`."
  [rdd f]
  (.mapToPair rdd (pair-function f)))

(defn map-to-double
  "Returns a new `JavaDoubleRDD` pairs by applying `f` to all elements of `rdd`."
  [rdd f]
  (.mapToDouble rdd (ff/double-function f)))

(defn flat-map
  "Similar to `map`, but each input item can be mapped to 0 or more output items (so the
  function `f` should return a collection rather than a single item)"
  [rdd f]
  (.flatMap rdd (flat-map-function f)))

(defn flat-map-to-pair
  "Returns a new `JavaPairRDD` by first applying `f` to all elements of `rdd`, and then flattening
  the results."
  [rdd f]
  (.flatMapToPair rdd (pair-flat-map-function f)))

(defn flat-map-to-double
  "Return a new JavaDoubleRDD by first applying a function to all elements of this RDD, and then flattening the results."
  [rdd f]
  (.flatMapToDouble rdd (ff/double-flat-map-function f)))

(defn map-partition
  "Similar to `map`, but runs separately on each partition (block) of the `rdd`, so function `f`
  must be of type Iterator<T> => Iterable<U>.
  https://issues.apache.org/jira/browse/SPARK-3369"
  [rdd f]
  (.mapPartitions rdd (flat-map-function f)))


(defn map-partitions-to-pair
  "Similar to `map`, but runs separately on each partition (block) of the `rdd`, so function `f`
  must be of type Iterator<T> => Iterable<U>.
  https://issues.apache.org/jira/browse/SPARK-3369"
  [rdd f & {:keys [preserves-partitioning]}]
  (.mapPartitionsToPair rdd (pair-flat-map-function f) (u/truthy? preserves-partitioning)))

(defn map-partition-with-index
  "Similar to `map-partition` but function `f` is of type (Int, Iterator<T>) => Iterator<U> where
  `i` represents the index of partition."
  [rdd f]
  (.mapPartitionsWithIndex rdd (function2 f) true))

(defn foreach
  "Applies the function `f` to all elements of `rdd`."
  [rdd f]
  (.foreach rdd (void-function f)))

(defn foreach-partition
  "Applies the function `f` to all elements of `rdd`."
  [rdd f]
  (.foreachPartition rdd (void-function f #_(comp f iterator-seq)))) ; todo fix this


(defn cartesian
  "Creates the cartesian product of two RDDs returning an RDD of pairs"
  [rdd1 rdd2]
  (.cartesian rdd1 rdd2))

(defn group-by
  "Returns an RDD of items grouped by the return value of function `f`."
  ([rdd f]
    (.groupBy rdd (function f)))
  ([rdd f n]
    (.groupBy rdd (function f) n)))

(defn take-sample
  "Returns a `fraction` sample of `rdd`, with or without replacement,
  using a given random number generator `seed`."
  [rdd with-replacement? fraction seed]
  (vec (.takeSample rdd with-replacement? fraction seed)))

;; ## Actions
;;
;; Action return their results to the driver process.
;;

(defn save-as-text-file
  "Writes the elements of `rdd` as a text file (or set of text files)
  in a given directory `path` in the local filesystem, HDFS or any other Hadoop-supported
  file system. Spark will call toString on each element to convert it to a line of
  text in the file."
  [rdd path]
  (.saveAsTextFile rdd path))

(defn save-as-sequence-file
  "Writes the elements of `rdd` as a Hadoop SequenceFile in a given `path`
  in the local filesystem, HDFS or any other Hadoop-supported file system.
  This is available on RDDs of key-value pairs that either implement Hadoop's
  Writable interface."
  [rdd path]
  (.saveAsSequenceFile rdd path))

(def first
  "Returns the first element of `rdd`."
  (memfn first))

(def count
  "Return the number of elements in `rdd`."
  (memfn count))

(def glom
  "Returns an RDD created by coalescing all elements of `rdd` within each partition into a list."
  (memfn glom))

(def collect
  "Returns all the elements of `rdd` as an ArrayList at the driver process."
  (memfn collect))

(defn take
  "Return an array with the first n elements of `rdd`.
  (Note: this is currently not executed in parallel. Instead, the driver
  program computes all the elements)."
  [rdd cnt]
  (.take rdd cnt))

(defn aggregate
  "Higher order function: aggregate(U zeroValue, Function2<U,T,U> seq-op, Function2<U,U,U> comb-op)
  First, for each partition, init aggregation-value with `zero-value` and then apply `seqOp` (taking 2 args, returning 1)
  to the aggregation-result and each next partition element till there's nothing left,
  ten do a flambo.api/reduce (initilised with zero-value) using `comb-op`(associative, commutative) on partition results.
  and apply `comb-op` the aggregation-value and each next partition result till there's nothing left"
  [rdd zero-value seq-op comb-op]
  (.aggregate rdd zero-value (function2 seq-op) (function2 comb-op)))

(defn fold-left
  "Higher order function:
  First, for each partition, init aggregation-value with `zero-value` and then apply `f` (taking 2 args, returning 1)
  to the aggregation-result and each next partition element till there's nothing left,
  ten do a flambo.api/reduce (initilised with zero-value) using `f`(this time associative, commutative) on partition results.
  Equivalent of (aggregate r zero-value f f).
  For commutative `f` reduce can be used instead (it's equivalent) and will be faster. "
  [rdd zero-value f]
  (.fold rdd zero-value (function2 f)))

(defn reduce
  "Applies the `f`(taking 2 args, returning 1) to each 2 elements (in no particular order) of the rdd and resulting values until there's one value left.
  The function should be commutative (a+b = b+a) and associative so that it can be
  computed correctly in parallel - faster than fold. For non-commutative `f` use fold."
  [rdd f]
  (.reduce rdd (function2 f)))

; tangramcare additions

(defn checkpoint
  "Mark this RDD for checkpointing."
  [rdd]
  (.checkpoint rdd))

(defn collect-async
  "The asynchronous version of collect, which returns a future for retrieving an ArrayList containing all of the elements in this RDD.
  This returns a regular java future, so all future related functions from clojure work."
  ^JavaFutureAction [rdd]
  (.collectAsync rdd))

(defn count-async
  "The asynchronous version of count, which returns a future for counting the number of elements in this RDD.
  This returns a regular java future, so all future related functions from clojure work."
  ^JavaFutureAction [rdd]
  (.countAsync rdd))

(defn foreach-async
  "The asynchronous version of the foreach action, which applies a function f to all the elements of this RDD."
  ^JavaFutureAction [rdd f]
  (.foreachAsync rdd (void-function f)))

(defn foreach-partition-async
  "The asynchronous version of the foreachPartition action, which applies a function f to each partition of this RDD.
  foreachPartitionAsync(VoidFunction<java.util.Iterator<T>> f)"
  ^JavaFutureAction [rdd f]
  (.foreachPartitionAsync rdd (void-function f)))

(defn take-largest
  "Returns the n largest elements from this RDD using the natural ordering for T, or ordering by comparator c.
   C can be a clojure fn (f/fn without aot) returning -1,0,1 because clojure fns implement java.util.comparator, or you can use flambo.api/comparator."
  ([rdd n]
    (.top rdd n))
  ([rdd n c]
    (.top rdd n (ff/comparator c))))

(defn take-smallest
  "Returns the n smallest elements from this RDD using the natural ordering for T, or ordering by comparator c.
   C can be a clojure fn (returning -1,0,1) because clojure fns implement java.util.comparator, or you can use flambo.api/comparator to create one or clojure.core.compare."
  ([rdd n]
    (.takeOrdered rdd n))
  ([rdd n c]
    (.takeOrdered rdd n (ff/comparator c))))

(defn take-sorted-by-pred
  "Returns the n items first in order of (pred x y) == true from this RDD."
  [rdd n pred]
  (.takeOrdered rdd n (ff/comparator (comparator pred))))

(defn take-seq
  "Return a clojure seq that contains all of the elements in this RDD.
  The seq will consume as much memory as the largest partition in this RDD."
  [rdd]
  (iterator-seq (.toLocalIterator rdd)))

(defn min
  "Returns the minimum element from this RDD as defined by the specified Comparator[T] (optional)
  C can be a clojure fn (returning -1,0,1) because clojure fns implement java.util.comparator, or you can use flambo.api/comparator to create one or clojure.core.compare."
  ([rdd]
    (.min rdd (ff/comparator compare)))
  ([rdd c]
    (.min rdd (ff/comparator c))))

(defn max
  "Returns the maximum element from this RDD as defined by the specified Comparator[T] (optional)
  C can be a clojure fn (returning -1,0,1) because clojure fns implement java.util.comparator, or you can use flambo.api/comparator to create one or clojure.core.compare."
  ([rdd]
    (.max rdd (ff/comparator compare)))
  ([rdd c]
    (.max rdd (ff/comparator c))))

(defn take-async
  "The asynchronous version of the take action, which returns a future for retrieving the first num elements of this RDD.
  This returns a regular java future, so all future related functions from clojure work."
  [rdd n]
  (.takeAsync rdd n))

(defn num-partitions
  "Returns number of partitions of this RDD."
  [rdd]
  (.size (.partitions rdd)))


(def zip
  "Zips this RDD with another one, returning PairRDD with the first element in each RDD, second element in each RDD, etc."
  (memfn zip))

(def zip-with-ndex
  "Zips this RDD with its element indices, returning PairRDD"
  (memfn zipWithIndex))

(def zip-with-unique-id
  "Zips this RDD with generated unique Long ids, returning PairRDD"
  (memfn zipWithUniqueId))

;;

(defprotocol PHasContext
  "Types capable of receiving spark context"
  (^JavaSparkContext get-java-spark-context [this]))

(extend-type SparkContext
  PHasContext
  (^JavaSparkContext get-java-spark-context [this]
    (JavaSparkContext/fromSparkContext this)))

(extend-type JavaRDDLike
  PHasContext
  (^JavaSparkContext get-java-spark-context [this]
    (JavaSparkContext/fromSparkContext (.context this))))

(defprotocol PConvertibleToJavaRDD
  "Types convertible to JavaRDD"
  (^JavaRDD to-java-rdd [this]))

(extend-type JavaRDD
  PConvertibleToJavaRDD
  (^JavaRDD to-java-rdd [this]
    this))

(extend-type JavaRDDLike
  PConvertibleToJavaRDD
  (^JavaRDD to-java-rdd [this]
    (JavaRDD/fromRDD (.rdd this) (.classTag this))))

(defprotocol PConvertibleToDoubleRDD
  "Types convertible to JavaDoubleRDD"
  (^JavaDoubleRDD to-java-double-rdd [this]))

(extend-type JavaDoubleRDD
  PConvertibleToDoubleRDD
  (^JavaDoubleRDD to-java-double-rdd [this]
    this))

(extend-type JavaRDDLike
  PConvertibleToDoubleRDD
  (^JavaDoubleRDD to-java-double-rdd [this]
    (if (= (.classTag this) k/DOUBLE-CLASS-TAG)
      (JavaDoubleRDD/fromRDD (.rdd this))
      (map-to-double this identity))))

(defprotocol PConvertibleToPairRDD
  "Types convertible to JavaPairRDD"
  (^JavaPairRDD to-java-pair-rdd [this]))

(extend-type JavaPairRDD
  PConvertibleToPairRDD
  (^JavaPairRDD to-java-pair-rdd [this]
    this))

(extend-type JavaRDDLike
  PConvertibleToPairRDD
  (^JavaPairRDD to-java-pair-rdd [this]
    (if (= (.classTag this) k/TUPLE-CLASS-TAG)
      (JavaPairRDD/fromRDD (.rdd this) k/FAKE-CLASS-TAG k/FAKE-CLASS-TAG)
      (map-to-pair this identity))))


(defprotocol PConvertibleToJavaRDD
  "Types convertible to JavaRDD"
  (^JavaRDD to-java-rdd [this]))

(extend-type JavaRDD
  PConvertibleToJavaRDD
  (^JavaRDD to-java-rdd [this]
    this))

(extend-type JavaRDDLike
  PConvertibleToJavaRDD
  (^JavaRDD to-java-rdd [this]
    (JavaRDD/fromRDD (.rdd this) (.classTag this))
    ))

;; JavaRDD common API

(def cache
  "Persists `rdd` with the default storage level (`MEMORY_ONLY`)."
  (memfn cache))

(defn count-partitions [rdd]
  (alength (.partitions (.rdd rdd))))

(defn partitions
  "Returns a vector of partitions for a given JavaRDD"
  [rdd]
  (into [] (.partitions (.rdd rdd))))

(defn hash-partitioner
  "Constructs a new hash partitionier"
  ([n]
    (HashPartitioner. n))
  ([subkey-fn n]
    (proxy [HashPartitioner] [n]
      (getPartition [key]
        (let [subkey (subkey-fn key)]
          (mod (hash subkey) n))))))

(defn coalesce
  "Decrease the number of partitions in `rdd` to `n`.
  Useful for running operations more efficiently after filtering down a large dataset."
  ([rdd n]
    (.coalesce rdd n))
  ([rdd n shuffle?]
    (.coalesce rdd n shuffle?)))

(defn coalesce-max
  "Decrease the number of partitions in `rdd` to `n`.
  Useful for running operations more efficiently after filtering down a large dataset."
  ([rdd n]
    (.coalesce rdd (min n (count-partitions rdd))))
  ([rdd n shuffle?]
    (.coalesce rdd (min n (count-partitions rdd)) shuffle?)))

(defn distinct
  "Return a new RDD that contains the distinct elements of the source `rdd`."
  ([rdd]
    (.distinct rdd))
  ([rdd n]
    (.distinct rdd n)))

(defn filter
  "Returns a new RDD containing only the elements of `rdd` that satisfy a predicate `f`."
  [rdd f]
  (.filter rdd (function (ftruthy? f))))

(defn intersection
  "Return the intersection of this RDD and another one.
  The output will not contain any duplicate elements, even if the input RDDs did.
  Note that this method performs a shuffle internally."
  [rdd other]
  (.intersection rdd other))

(defn persist
  "Sets the storage level of `rdd` to persist its values across operations
  after the first time it is computed. storage levels are available in the `STORAGE-LEVELS' map.
  This can only be used to assign a new storage level if the RDD does not have a storage level set already."
  [rdd storage-level]
  (.persist rdd storage-level))

(defn repartition
  "Returns a new `rdd` with exactly `n` partitions.
  Can increase or decrease the level of parallelism in this RDD. Internally, this uses a shuffle to redistribute data.
  If you are decreasing the number of partitions in this RDD, consider using coalesce, which can avoid performing a shuffle."
  [rdd n]
  (.repartition rdd n))

(defn sample
  "Return a sampled subset of this RDD."
  ([rdd with-replacement? fraction]
    (.sample rdd with-replacement? (double fraction)))
  ([rdd with-replacement? fraction seed]
    (.sample rdd with-replacement? (double fraction) (long seed))))

(defn rdd-name
  "Assign a name to this RDD"
  ([rdd name]
    (.setName rdd name))
  ([rdd]
    (.name rdd)))

(defn subtract
  "Return an RDD with the elements from rdd that are not in other.
  By default uses this partition size, because even if other is huge, the resulting RDD will be <= us."
  ([rdd other-rdd]
    (.subtract rdd other-rdd))
  ([rdd other-rdd partitions]
    (.subtract rdd other-rdd partitions)))

(defprotocol PUnionCapableRDD
  "Types convertible to JavaRDD"
  (union-impl [rdd rdds]))

(extend-type JavaRDDLike
  PUnionCapableRDD
  (union-impl [rdd rdds]
    (.union (get-java-spark-context rdd) rdd (java.util.ArrayList. rdds))))

(defn union
  "Return the union of this RDD and others. Any identical elements will appear multiple times (use api/distinct to eliminate them)."
  ([rdd]
    rdd)
  ([rdd & rdds]
    (union-impl rdd rdds))
  )

(defn unpersist
  "Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
  This method blocks until all blocks are deleted (by default)."
  ([rdd]
    (.unpersist rdd))
  ([rdd blocking?]
    (.unpersist rdd blocking?)))


; JavaRDD specific api:
(defn random-split
  "Randomly splits this RDD with the provided weights.
  weights - weights for splits, will be normalized if they don't sum to 1
  seed (optional) - random seed
  returns: split RDDs in an array"
  ([rdd weights]
    (vec (.randomSplit (to-java-rdd rdd) (double-array weights))))
  ([rdd weights seed]
    (vec (.randomSplit (to-java-rdd rdd) (double-array weights) (long seed)))))

(defn sort-by
  "Return this RDD sorted by the given key function."
  ([rdd f]
    (.sortBy (to-java-rdd rdd) (function f) true (num-partitions rdd)))
  ([rdd f ascending?]
    (.sortBy (to-java-rdd rdd) (function f) ascending? (num-partitions rdd)))
  ([rdd f ascending? num-partitions]
    (.sortBy (to-java-rdd rdd) (function f) ascending? num-partitions)))

; JavaDoubleRDD API

(defmulti histogram "compute histogram of an RDD of doubles"
          (fn [_ bucket-arg] (sequential? bucket-arg)))

(defmethod histogram true [rdd buckets]
  (let [counts (-> (to-java-double-rdd rdd)
                   (.histogram (double-array buckets)))]
    (into [] counts)))

(defmethod histogram false [rdd bucket-count]
  (let [[buckets counts] (-> (to-java-double-rdd rdd)
                             (.histogram bucket-count)
                             fi/untuple)]
    [(into [] buckets) (into [] counts)]))

(defn mean
  "Compute the mean of this RDD's elements."
  [rdd]
  (.mean (to-java-double-rdd rdd)))

(defn sample-stdev
  "Compute the sample standard deviation of this RDD's elements (which corrects for bias in estimating the standard deviation by dividing by N-1 instead of N)."
  [rdd]
  (.sampleStdev (to-java-double-rdd rdd)))

(defn sample-variance
  "Compute the sample variance of this RDD's elements (which corrects for bias in estimating the standard variance by dividing by N-1 instead of N)."
  [rdd]
  (.sampleVariance (to-java-double-rdd rdd)))

(defn stats
  "Return a stats map that captures the mean, variance and count of the RDD's elements in one operation."
  [rdd]
  (let [s (.stats (to-java-double-rdd rdd))]
    {:count           (.count s)
     :max             (.max s)
     :min             (.min s)
     :mean            (.mean s)
     :sum             (.sum s)
     :stdev           (.stdev s)
     :variance        (.variance s)
     :sample-stdev    (.sampleStdev s)
     :sample-variance (.sampleVariance s)}))

(defn stdev
  "Compute the standard deviation of this RDD's elements."
  [rdd]
  (.stdev (to-java-double-rdd rdd)))

(defn sum
  "Add up the elements in this RDD."
  [rdd]
  (.sum (to-java-double-rdd rdd)))

(defn variance
  "Compute the variance of this RDD's elements."
  [rdd]
  (.variance (to-java-double-rdd rdd)))

;; JavaPairRDD specific API

(defn cogroup
  "For each key k in this or other, return a resulting RDD that contains a tuple with the list of values for that key in this as well as other."
  ([^JavaPairRDD rdd ^JavaPairRDD other]
    (.cogroup rdd other))
  ([^JavaPairRDD rdd ^JavaPairRDD other1 ^JavaPairRDD other2]
    (.cogroup rdd
              other1
              other2)))

(defn reduce-by-key
  "When called on an `rdd` of (K, V) pairs, returns an RDD of (K, V) pairs
  where the values for each key are aggregated using the given reduce function `f`."
  [rdd f]
  (-> rdd
      (to-java-pair-rdd)
      (.reduceByKey (function2 f))))

(defn group-by-key
  "Groups the values for each key in `rdd` into a single sequence."
  ([rdd]
    (.groupByKey (to-java-pair-rdd rdd)))
  ([rdd n]
    (.groupByKey (to-java-pair-rdd rdd) n)))

(defn combine-by-key
  "Combines the elements for each key using a custom set of aggregation functions.
  Turns an RDD of (K, V) pairs into a result of type (K, C), for a 'combined type' C.
  Note that V and C can be different -- for example, one might group an RDD of type
  (Int, Int) into an RDD of type (Int, List[Int]).
  Users must provide three functions:
  -- createCombiner, which turns a V into a C (e.g., creates a one-element list)
  -- mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
  -- mergeCombiners, to combine two C's into a single one."
  ([rdd create-combiner merge-value merge-combiners]
    (.combineByKey (to-java-pair-rdd rdd)
                   (function create-combiner)
                   (function2 merge-value)
                   (function2 merge-combiners)))
  ([rdd create-combiner merge-value merge-combiners n]
    (.combineByKey (to-java-pair-rdd rdd)
                   (function create-combiner)
                   (function2 merge-value)
                   (function2 merge-combiners)
                   n)))

(defn sort-by-key
  "When called on `rdd` of (K, V) pairs where K implements ordered, returns a dataset of
   (K, V) pairs sorted by keys in ascending or descending order, as specified by the boolean
  ascending argument."
  ([rdd]
    (sort-by-key rdd compare true))
  ([rdd x]
    ;; RDD has a .sortByKey signature with just a Boolean arg, but it doesn't
    ;; seem to work when I try it, bool is ignored.
    (if (instance? Boolean x)
      (sort-by-key rdd compare x)
      (sort-by-key rdd x true)))
  ([rdd compare-fn asc?]
    (-> rdd
        (to-java-pair-rdd)
        (.sortByKey
          (ff/comparator compare-fn)
          (u/truthy? asc?)))))

(defn join
  "When called on `rdd` of type (K, V) and (K, W), returns a dataset of
  (K, (V, W)) pairs with all pairs of elements for each key."
  [rdd other]
  (.join (to-java-pair-rdd rdd) (to-java-pair-rdd other)))

(defn left-outer-join
  "Performs a left outer join of `rdd` and `other`. For each element (K, V)
   in the RDD, the resulting RDD will either contain all pairs (K, (V, W)) for W in other,
  or the pair (K, (V, nil)) if no elements in other have key K."
  [rdd other]
  (.leftOuterJoin (to-java-pair-rdd rdd) (to-java-pair-rdd other)))

(defn count-by-key
  "Only available on RDDs of type (K, V).
  Returns a map of (K, Int) pairs with the count of each key."
  [rdd]
  (into {}
        (.countByKey (to-java-pair-rdd rdd))))

(defn values
  "for given PairRDD returns JavaRDD of values of all pairs"
  [rdd]
  (-> rdd
      (to-java-pair-rdd)
      (.values)))

(defn keys
  "for given PairRDD returns JavaRDD of keys of all pairs"
  [rdd]
  (-> rdd
      (to-java-pair-rdd)
      (.keys)))

(defn aggregate-by-key
  "Aggregate the values of each key, using given combine functions and a neutral 'zero value'
  returns a JavaPairRDD of [key flambo.api/aggregate on key values]"
  ([rdd zero-value seq-op comb-op]
    (.aggregateByKey (to-java-pair-rdd rdd) zero-value (function2 seq-op) (function2 comb-op)))
  ([rdd zero-value partitions seq-op comb-op]
    (.aggregateByKey (to-java-pair-rdd rdd) zero-value partitions (function2 seq-op) (function2 comb-op))))

(defn fold-left-by-key
  "Fold-left the values of each key, using given combine functions and a neutral 'zero value'
  returns a JavaPairRDD of [key flambo.api/fold-left on key values]"
  ([rdd zero-value f]
    (.foldByKey (to-java-pair-rdd rdd) zero-value (function2 f)))
  ([rdd zero-value partitions f]
    (.foldByKey (to-java-pair-rdd rdd) zero-value partitions (function2 f))))

(defn subtract-by-key
  "Return an RDD with the pairs from this whose keys are not in other..
  By default uses this partition size, because even if other is huge, the resulting RDD will be <= us."
  ([rdd other-rdd]
    (.subtractByKey (to-java-pair-rdd rdd) (to-java-pair-rdd other-rdd)))
  ([rdd other-rdd partitions]
    (.subtractByKey (to-java-pair-rdd rdd) (to-java-pair-rdd other-rdd) partitions)))

(defn map-values
  "Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD's partitioning."
  [rdd f]
  (.mapValues (to-java-pair-rdd rdd) (ff/function f)))

(defn flat-map-values
  "Pass each value in the key-value pair RDD through a flatMap function without changing the keys; this also retains the original RDD's partitioning."
  [rdd f]
  (.flatMapValues (to-java-pair-rdd rdd) (ff/function f)))

(defn collect-as-map
  "Return the key-value pairs in this RDD to the master as a Map."
  [rdd]
  (into {}
        (.collectAsMap (to-java-pair-rdd rdd))))

(defn partition-by
  [^JavaPairRDD rdd ^Partitioner partitioner]
  (.partitionBy rdd partitioner))

(defn lookup
  "Return the list of values in the RDD for key."
  [rdd key]
  (.lookup (to-java-pair-rdd rdd) key))

(defn partitioner [^JavaPairRDD rdd]
  (fi/some-or-nil (.partitioner (.rdd rdd))))

(defn rekey-preserving-partitioning-without-check
  "This re-keys a pair-rdd by applying the rekey-fn to generate new tuples. However, it does not check whether your new keys would keep the same partitioning, so watch out!!!!"
  [rdd rekey-fn]
  (map-partitions-to-pair
    rdd
    (fn [iterator]
        (clojure.core/map rekey-fn
                          (iterator-seq iterator)))
    :preserves-partitioning true
    ))

;; ColsRDD specific API

;TODO: impl some ops for cols rdd
;TODO: map-to-cols, flat-map-to-cols, use-cols-indexes, use-cols-idxfns