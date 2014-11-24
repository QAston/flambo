(ns flambo.mllib
  (:import (org.apache.spark.mllib.linalg.distributed CoordinateMatrix RowMatrix))
  (:refer-clojure :exclude [vec])
  (:import (org.apache.spark.mllib.linalg Vector Vectors Matrices Matrix)
           (org.apache.spark.mllib.linalg.distributed DistributedMatrix CoordinateMatrix RowMatrix IndexedRow IndexedRowMatrix)))

;;linear algebra stuff

(defn dense-vec
  "Vector has integer-typed and 0-based indices and double-typed values, stored on a single machine.
  A dense vector is backed by a double array representing its entry values.

  Usage:
  (dense-vec (range 0 100))"
  [coll]
  (Vectors/dense (double-array coll)))

(defn sparse-vec
  "Vector has integer-typed and 0-based indices and double-typed values, stored on a single machine.
  A sparse vector is backed by two parallel arrays: indices and values.

  Usage:
  (sparse-vec 100
              (filter #(== (mod % 2) 0) ; indexes 0, 2, 4, 6,...
                           (range 0 100))
              (range 0 50)) ; numbers 0 - 50
  ;=> [0.0, 0.0, 1.0, 0.0, 2.0, 0.0 3.0, ..."
  [len indexes vals]
  (Vectors/sparse len (int-array indexes) (double-array vals)))

(defn vec
  "converts sparse/dense vector or dense matrix to clojure vector

  Usage:
  (vec (dense-vec (range 0 100)))"
  [v]
  (clojure.core/vec (.toArray v)))

(defn dense-matrix
  "Column-majored dense matrix filled with doubles from values-column-at-a-time one column at a time

  Usage:
  (dense-matrix (range 0 4) 2 2) ; returns [[0.0 2.0]
  [1.0 2.0]]"
  [values-column-at-a-time num-rows, num-cols]
  (Matrices/dense num-rows num-cols (double-array values-column-at-a-time))
  )
