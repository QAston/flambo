(ns flambo.function
  (:refer-clojure :exclude [comparator fn])
  (:require [serializable.fn :as sfn]
            [flambo.utils :as u]
            [flambo.kryo :as kryo]
            [clojure.tools.logging :as log])
  (:import [scala Tuple2]
           (flambo.function ScalaFunction0 ScalaFunction1)))

(defmacro fn
  [& body]
  `(sfn/fn ~@body))

(defn- serfn? [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)
(def deserialize-fn (memoize sfn/deserialize))
(def array-of-bytes-type (Class/forName "[B"))

;; ## Generic
(defn -init
  "Save the function f in state"
  [f]
  [[] f])

(defn -call [this & xs]
  (let [fn-or-serfn (.state this)
        f (if (instance? array-of-bytes-type fn-or-serfn)
            (binding [sfn/*deserialize* kryo/deserialize]
              (deserialize-fn fn-or-serfn))
            fn-or-serfn)]
    (log/trace "CLASS" (type this))
    (log/trace "META" (meta f))
    (log/trace "XS" xs)
    (apply f xs)))

;; ## Functions
(defn mk-sym
  [fmt sym-name]
  (symbol (format fmt sym-name)))

(defmacro gen-function+class
  [clazz wrapper-name fn-symbol implemented-type]
    (let [new-class-sym (mk-sym "flambo.function.%s" clazz)
          prefix-sym (mk-sym "%s-" clazz)]
      `(do
         (def ~(mk-sym "%s-init" clazz) -init)
         (def ~fn-symbol -call)
         (gen-class
           :name ~new-class-sym
           :implements [~implemented-type java.io.Serializable]
           :prefix ~prefix-sym
           :init ~'init
           :state ~'state
           :constructors {[Object] []})
         (defn ~wrapper-name [f#]
           (new ~new-class-sym
                (if (serfn? f#) (binding [sfn/*serialize* kryo/serialize]
                                  (serialize-fn f#)) f#))))))

(defmacro gen-spark-api-function
  [clazz wrapper-name]
    `(gen-function+class ~clazz ~wrapper-name ~(mk-sym "%s-call" clazz) ~(mk-sym "org.apache.spark.api.java.function.%s" clazz)))
#_(defmacro gen-spark-api-function
  [clazz wrapper-name]

  `(defn ~wrapper-name [f#]
     (new ~(symbol (str "flambo.function.Flambo" clazz)) f#)))

(gen-function+class Comparator comparator Comparator-compare java.util.Comparator)
;
(gen-spark-api-function Function function)
(gen-spark-api-function Function2 function2)
(gen-spark-api-function Function3 function3)
(gen-spark-api-function VoidFunction void-function)
(gen-spark-api-function FlatMapFunction flat-map-function)
(gen-spark-api-function FlatMapFunction2 flat-map-function2)
(gen-spark-api-function PairFlatMapFunction pair-flat-map-function)
(gen-spark-api-function PairFunction pair-function)
(gen-spark-api-function DoubleFlatMapFunction double-flat-map-function) ; A function that takes T, returns zero or more records of type Double from each input record.
(gen-spark-api-function DoubleFunction double-function)     ; A function that takes T, returns Doubles, and can be used to construct DoubleRDDs.

;; Replaces the PairFunction-call and PairFlatMapFunction-call defined by the gen-function macro.
(defn PairFunction-call [this x]
  (let [[a b] (-call this x)]
    (Tuple2. a b)))

(defn PairFlatMapFunction-call [this x]
  (let [ret (-call this x)]
    (for [v ret
          :let [[a b] v]]
      (Tuple2. a b))))

(defn DoubleFunction-call [this x]
  (double (-call this x)))

(defn DoubleFlatMapFunction-call [this x]
  (map double (-call this x)))

(defmacro gen-function
  [clazz wrapper-name]

  `(defn ~wrapper-name [f#]
     (new ~(symbol (str "flambo.function." clazz)) f#)))

(gen-function ScalaFunction0 scala-function0)
(gen-function ScalaFunction1 scala-function1)
