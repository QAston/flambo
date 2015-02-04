(ns flambo.interop
  (:refer-clojure :exclude [first second seq vec fn])
  (:require [flambo.function :refer [fn]])
  (:import [scala Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9 Tuple10 Tuple11 Tuple12 Tuple13 Tuple14 Tuple15 Tuple16 Tuple17 Tuple18 Tuple19 Tuple20 Tuple21 Tuple22 Some]
           [com.google.common.base Optional])
  )

(defprotocol ClojureTuple2
  (first [this])
  (second [this])
  (vec [this])
  (seq [this]))

(extend-type Tuple2
  ClojureTuple2
  (first [this]
    (._1 this))
  (second [this]
    (._2 this))
  (vec [this]
    (let [v (transient [])]
      (conj! v (first this))
      (conj! v (second this))
      (persistent! v)))
  (seq [this]
    (clojure.core/seq (vec this))))

(defn key-val-fn
  "wraps a function f [k v] to untuple a key/value tuple. Useful e.g. on map for PairRDD."
  [f]
  (fn [^Tuple2 t]
    (f (._1 t) (._2 t))))

(defn key-seq-seq-fn
  "wraps a function f [k seq1 seq2] to untuple a key/value tuple with two partial values both being seqs. Useful e.g. on map after a cogroup with two RDDs."
  [f]
  (fn [^Tuple2 t]
    (let [k (._1 t)
          v ^Tuple2 (._2 t)]
      (f k (seq (._1 v)) (seq (._2 v))))))

(defn seq-seq-fn
  "wraps a function f [seq1 seq2] to untuple a tuple-value with two partial values all being seqs. Useful e.g. on map-values after a cogroup with two RDDs."
  [f]
  (fn [^Tuple2 t]
    (f (seq (._1 t)) (seq (._2 t)))))

(defn key-seq-seq-seq-fn [f]
  "wraps a function f [k seq1 seq2 seq3] to untuple a key/value tuple with three partial values all being seqs. Useful e.g. on map after a cogroup with three RDDs."
  (fn [^Tuple2 t]
    (let [k (._1 t)
          v ^Tuple3 (._2 t)]
      (f k (seq (._1 v)) (seq (._2 v)) (seq (._3 v))))))

(defn seq-seq-seq-fn
  "wraps a function f [seq1 seq2 seq3] to untuple a triple-value with three partial values all being seqs. Useful e.g. on map-values after a cogroup with three RDDs."
  [f]
  (fn [^Tuple3 v]
    (f (seq (._1 v)) (seq (._2 v)) (seq (._3 v)))))

(defn- second-value [^Tuple2 t]
  (._2 t))

(defn- optional-second-value [^Tuple2 t]
  (.orNull ^Optional (._2 t)))

(defn key-val-val-fn
  "wraps a function f [k val1 val2] to untuple a key/value tuple with two partial values. Useful e.g. on map after a join. If optional-second-value? is true (default is false), the second tuple-value might not be there (by use of Optional), e.g. in joins, where the right side might be empty. We'll just call the wrapped function with nil as second value then."
  [f & {:keys [optional-second-value?] :or {optional-second-value? false}}]
  (let [second-value-fn (if optional-second-value?
                          optional-second-value
                          second-value)]
    (fn [^Tuple2 t]
      (let [k (._1 t)
            v ^Tuple2 (._2 t)
            v1 (._1 v)
            v2 (second-value-fn v)]
        (f k v1 v2)))))


(defn val-val-fn
  "wraps a function f [val1 val2] to untuple a value tuple with two partial values. Useful e.g. on map-values after a join. If optional-second-value? is true (default is false), the second tuple-value might not be there (by use of Optional), e.g. in joins, where the right side might be empty. We'll just call the wrapped function with nil as second value then."
  [f & {:keys [optional-second-value?] :or {optional-second-value? false}}]
  (let [second-value-fn (if optional-second-value?
                          optional-second-value
                          second-value)]
    (fn [^Tuple2 v]
      (let [v1 (._1 v)
            v2 (second-value-fn v)]
        (f v1 v2)))))

(defn tuple
  "Returns a Scala tuple. Uses the Scala tuple class that matches the
  number of arguments.
  ($/tuple 1 2) => (instance-of scala.Tuple2)
  (apply $/tuple (range 20)) => (instance-of scala.Tuple20)
  (apply $/tuple (range 21)) => (instance-of scala.Tuple21)
  (apply $/tuple (range 22)) => (instance-of scala.Tuple22)
  (apply $/tuple (range 23)) => (throws ExceptionInfo)"
  {:added "0.1.0"}
  ([a]
    (Tuple1. a))
  ([a b]
    (Tuple2. a b))
  ([a b c]
    (Tuple3. a b c))
  ([a b c d]
    (Tuple4. a b c d))
  ([a b c d e]
    (Tuple5. a b c d e))
  ([a b c d e f]
    (Tuple6. a b c d e f))
  ([a b c d e f g]
    (Tuple7. a b c d e f g))
  ([a b c d e f g h]
    (Tuple8. a b c d e f g h))
  ([a b c d e f g h i]
    (Tuple9. a b c d e f g h i))
  ([a b c d e f g h i j]
    (Tuple10. a b c d e f g h i j))
  ([a b c d e f g h i j k]
    (Tuple11. a b c d e f g h i j k))
  ([a b c d e f g h i j k l]
    (Tuple12. a b c d e f g h i j k l))
  ([a b c d e f g h i j k l m]
    (Tuple13. a b c d e f g h i j k l m))
  ([a b c d e f g h i j k l m n]
    (Tuple14. a b c d e f g h i j k l m n))
  ([a b c d e f g h i j k l m n o]
    (Tuple15. a b c d e f g h i j k l m n o))
  ([a b c d e f g h i j k l m n o p]
    (Tuple16. a b c d e f g h i j k l m n o p))
  ([a b c d e f g h i j k l m n o p q]
    (Tuple17. a b c d e f g h i j k l m n o p q))
  ([a b c d e f g h i j k l m n o p q r]
    (Tuple18. a b c d e f g h i j k l m n o p q r))
  ([a b c d e f g h i j k l m n o p q r s]
    (Tuple19. a b c d e f g h i j k l m n o p q r s))
  ([a b c d e f g h i j k l m n o p q r s t]
    (Tuple20. a b c d e f g h i j k l m n o p q r s t))
  ([a b c d e f g h i j k l m n o p q r s t & [u v :as args]]
    (case (count args)
      1 (Tuple21. a b c d e f g h i j k l m n o p q r s t u)
      2 (Tuple22. a b c d e f g h i j k l m n o p q r s t u v)
      (throw (ex-info "Can only create Scala tuples with up to 22 elements"
                      {:count (+ 20 (count args))})))))

(defn to-tuple
  "Converts given coll to tuple."
  [coll]
  (apply tuple coll))

(defmethod print-method Tuple2 [^Tuple2 o ^java.io.Writer w]
  (.write w (str "#flambo.interop/tuple " (pr-str [(._1 o) (._2 o)]))))

(defmethod print-dup Tuple2 [o w]
  (print-method o w))

(defn some-or-nil [option]
  (when (instance? Some option)
    (.get ^Some option)))

(defn untuple
  "Converts (tuple key val) to [key val]"
  [^Tuple2 t]
  (let [v (transient [])]
    (conj! v (._1 t))
    (conj! v (._2 t))
    (persistent! v)))

(defn double-untuple
  "Converts (tuple key (tuple v1 v2)) to [key [v1 v2]]"
  [^Tuple2 t]
  (let [[x ^Tuple2 t2] (untuple t)
        v (transient [])]
    (conj! v x)
    (conj! v (untuple t2))
    (persistent! v)))

(defn group-untuple
  "Converts (tuple key (v1 v2 v3..)) to [key [v1 v2 v3...]]"
  [^Tuple2 t]
  (let [v (transient [])]
    (conj! v (._1 t))
    (conj! v (into [] (._2 t)))
    (persistent! v)))


(defn left-outer-join-untuple [^Tuple2 t]
  (let [[x t2] (untuple t)
        [a b] (untuple t2)]
    (vector x [a (.orNull b)])))

(defn seq-value [[k v]]
  [k (seq v)])

(defn untuple-value [[k v]]
  [k (untuple v)])