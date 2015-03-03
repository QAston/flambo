(ns flambo.function
  (:refer-clojure :exclude [comparator fn])
  (:require [serializable.fn :as sfn]
            [flambo.utils :as u]
            [flambo.kryo :as kryo]
            [clojure.tools.logging :as log])
  (:import [scala Tuple2]
           (java.io ObjectOutputStream ObjectInputStream)))

(defmacro fn
  [& body]
  `(sfn/fn ~@body))

(defn- serfn? [f]
  (= (type f) :serializable.fn/serializable-fn))

(def serialize-fn sfn/serialize)
(def deserialize-fn (memoize sfn/deserialize))
(def array-of-bytes-type (Class/forName "[B"))

;; ## Functions
(defn mk-sym
  [fmt sym-name]
  (symbol (format fmt sym-name)))

(defn write-to-output-stream [^clojure.lang.AFunction$1 afn ^ObjectOutputStream out]
  (let [b (binding [sfn/*serialize* kryo/serialize]
            (serialize-fn afn))]
    (log/trace "writing fn:" afn "serialized:" (String. b) "length:" (alength b))
    (.writeObject out b)))

(defn ^clojure.lang.AFunction$1 read-from-input-stream [^ObjectInputStream in]
  (let [b (.readObject in)]
    (log/trace "reading fn serialized:" (String. b) "length:" (alength b))
    (binding [sfn/*deserialize* kryo/deserialize]
      (deserialize-fn b))))


(defn -jit-init
  [f]
  [[f]])

(defn -jit-call [this & xs]
  (apply ^clojure.lang.AFunction$1 (.f this) xs))

(defmacro gen-jit-function-class
  [clazz method-symbol base-class interface]
  (let [new-class-sym (mk-sym "flambo.jitfn.%s" clazz)
        prefix-sym (mk-sym "%s-jit-" clazz)]
    `(do
       (def ~(mk-sym "%s-jit-init" clazz) -jit-init)
       (def ~(mk-sym (str "%s-jit-" method-symbol) clazz) -jit-call)
       (gen-class
         :name ~new-class-sym
         :implements [java.io.Serializable ~interface]
         :extends ~base-class
         :prefix ~prefix-sym
         :init ~'init
         :constructors ~'{[Object] [Object]}
         :exposes ~'{f {:get f}})
       )))

(defn -aot-init
  [f]
  [[f]])

(defn -aot-call [this & xs]
  (apply ^clojure.lang.AFunction (.f this) xs))

(defmacro gen-aot-function-class
  [clazz method-symbol base-class interface]
  (let [new-class-sym (mk-sym "flambo.aotfn.%s" clazz)
        prefix-sym (mk-sym "%s-aot-" clazz)]
    `(do
       (def ~(mk-sym "%s-aot-init" clazz) -aot-init)
       (def ~(mk-sym (str "%s-aot-" method-symbol) clazz) -aot-call)
       (gen-class
         :name ~new-class-sym
         :implements [java.io.Serializable ~interface]
         :extends ~base-class
         :prefix ~prefix-sym
         :init ~'init
         :constructors ~'{[clojure.lang.AFunction] [clojure.lang.AFunction]}
         :exposes ~'{f {:get f}})
       )))

(defmacro gen-jit-aot-call-wrappers
  [clazz wrapper-name method-name interface]
  `(do
     (gen-jit-function-class ~clazz ~method-name ~'flambo.serialize.AbstractSerializableWrappedAFunctionJit ~interface)
     (gen-aot-function-class ~clazz ~method-name ~'flambo.serialize.AbstractSerializableWrappedAFunctionAot ~interface)
     (defn ~wrapper-name
       [f#]
       (if (serfn? f#)
         (~(mk-sym "flambo.jitfn.%s." clazz) f#)
         (~(mk-sym "flambo.aotfn.%s." clazz) f#)))))

(defmacro gen-spark-api-function
  [clazz wrapper-name]
  `(do
     (gen-jit-function-class ~(symbol (str "Flambo" clazz)) ~'call ~'flambo.serialize.AbstractSerializableWrappedAFunctionJit ~(mk-sym "org.apache.spark.api.java.function.%s" clazz))
     (gen-aot-function-class ~(symbol (str "Flambo" clazz)) ~'call ~'flambo.serialize.AbstractSerializableWrappedAFunctionAot ~(mk-sym "org.apache.spark.api.java.function.%s" clazz))
     (defn ~wrapper-name
       [f#]
       (if (serfn? f#)
         (~(mk-sym "flambo.jitfn.Flambo%s." clazz) f#)
         (~(mk-sym "flambo.aotfn.Flambo%s." clazz) f#)))))

(gen-jit-aot-call-wrappers Comparator comparator compare java.util.Comparator)
;
(gen-spark-api-function Function function)
(gen-spark-api-function Function2 function2)
(gen-spark-api-function Function3 function3)
(gen-spark-api-function VoidFunction void-function)
(gen-spark-api-function FlatMapFunction flat-map-function)
(gen-spark-api-function FlatMapFunction2 flat-map-function2)
(gen-spark-api-function PairFlatMapFunction pair-flat-map-function)
(gen-spark-api-function PairFunction pair-function)
(gen-spark-api-function DoubleFlatMapFunction double-flat-map-function)
(gen-spark-api-function DoubleFunction double-function)

;; Replaces the PairFunction-call and PairFlatMapFunction-call defined by the gen-function macro.
(defn FlamboPairFunction-jit-call [this x]
  (let [[a b] (-jit-call this x)]
    (Tuple2. a b)))

(defn FlamboPairFlatMapFunction-jit-call [this x]
  (let [ret (-jit-call this x)]
    (for [v ret
          :let [[a b] v]]
      (Tuple2. a b))))

(defn FlamboDoubleFunction-jit-call [this x]
  (double (-jit-call this x)))

(defn FlamboDoubleFlatMapFunction-jit-call [this x]
  (map double (-jit-call this x)))


;; Replaces the PairFunction-call and PairFlatMapFunction-call defined by the gen-function macro.
(defn FlamboPairFunction-aot-call [this x]
  (let [[a b] (-aot-call this x)]
    (Tuple2. a b)))

(defn FlamboPairFlatMapFunction-aot-call [this x]
  (let [ret (-aot-call this x)]
    (for [v ret
          :let [[a b] v]]
      (Tuple2. a b))))

(defn FlamboDoubleFunction-aot-call [this x]
  (double (-aot-call this x)))

(defn FlamboDoubleFlatMapFunction-aot-call [this x]
  (map double (-aot-call this x)))

(defmacro gen-scala-function
  [clazz wrapper-name]

  `(defn ~wrapper-name [f#]
     (new ~(symbol (str "flambo.aotfna." clazz)) f#)))

(gen-scala-function ScalaFunction0 scala-function0)
(gen-scala-function ScalaFunction1 scala-function1)
