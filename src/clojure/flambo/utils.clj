(ns flambo.utils
  (:require [clojure.tools.logging :as log])
  (:import [scala Tuple2]
           [org.apache.spark.util.random BernoulliCellSampler XORShiftRandom]
           [java.io PrintStream]
           [org.apache.log4j Logger WriterAppender SimpleLayout]))

(defn echo-types [c]
  (if (coll? c)
    (log/debug "TYPES" (map type c))
    (log/debug "TYPES" (type c)))
  c)

(defn trace [msg]
  (fn [x]
    (log/trace msg x)
    x))

(defn truthy? [x]
  (if x (Boolean/TRUE) (Boolean/FALSE)))

(defn as-integer [s]
  (Integer. s))

(defn as-long [s]
  (Long. s))

(defn as-double [s]
  (Double. s))

(defn bernoulli-sampler [lower-bound upper-bound complement?]
  (BernoulliCellSampler. lower-bound upper-bound complement?))

(defn sampler-complement [sampler]
  (.cloneComplement sampler))