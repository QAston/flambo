(ns flambo.mllib
  (:import (org.apache.spark.mllib.linalg.distributed CoordinateMatrix RowMatrix))
  (:refer-clojure :exclude [vec])
  (:import (org.apache.spark.mllib.linalg Vector Vectors Matrices Matrix)
           (org.apache.spark.mllib.linalg.distributed DistributedMatrix CoordinateMatrix RowMatrix IndexedRow IndexedRowMatrix MatrixEntry)
           (org.apache.spark.mllib.regression LabeledPoint)))

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

(defn row-matrix
  "A RowMatrix is a row-oriented distributed matrix without meaningful row indices,
  backed by an RDD of its rows, where each row is a local vector.
  Since each row is represented by a local vector, the number of columns is limited
  by the integer range but it should be much smaller in practice."
  ([row-vector-rdd num-rows num-cols]
    (RowMatrix. row-vector-rdd num-rows num-cols))
  ([row-vector-rdd]
    (RowMatrix. row-vector-rdd)
    ))

(defn indexed-row-matrix
  "An IndexedRowMatrix is similar to a RowMatrix but with meaningful row indices.
  It is backed by an RDD of indexed rows,
  so that each row is represented by its index (long-typed) and a local vector."
  ([indexed-row-rdd num-rows num-cols]
    (IndexedRowMatrix. indexed-row-rdd num-rows num-cols))
  ([indexed-row-rdd]
    (IndexedRowMatrix. indexed-row-rdd)
    ))

(defn indexed-row
  "Entry for indexed-row-matrix rdd"
  [vector index]
  (IndexedRow. index vector))

(defn matrix-entry
  "Entry for coordinate-matrix rdd"
  [i j value]
  (MatrixEntry. i j value))

(defn coordinate-matrix
  "A CoordinateMatrix is a distributed matrix backed by an RDD of its entries.
  Each entry is a tuple of (i: Long, j: Long, value: Double),
  where i is the row index, j is the column index, and value is the entry value.
  A CoordinateMatrix should be used only when both dimensions of the matrix are huge
  and the matrix is very sparse."
  ([entry-rdd num-rows num-cols]
    (CoordinateMatrix. entry-rdd num-rows num-cols))
  ([entry-rdd]
    (CoordinateMatrix. entry-rdd)
    ))

(defn labeled-point
  "A labeled point is a local vector, either dense or sparse, associated with a label/response - Double"
  [vector label]
  (LabeledPoint. label vector))
