(ns flambo.mllib-test
  (:use midje.sweet)
  (:require [flambo.mllib :as ml]))

(facts
  "about dense vector"
  (fact
    "can construct from a seq"
    (.size (ml/dense-vec (range 0 100))) => 100)
  (fact
    "can convert to vector on demand"
    (ml/vec (ml/dense-vec (range 0 100))) => (vec (range 0.0 100))))

(facts
  "about sparse vector"
  (fact
    "can construct from a seq"
    (.size (ml/sparse-vec 100
                          (filter #(== (mod % 2) 0)
                                  (range 0 100))
                          (range 0 50)))
    => 100)
  (fact
    "can convert to vector on demand"
    (ml/vec (ml/sparse-vec 100
                           (filter #(== (mod % 2) 0) (range 0 100))
                           (range 0 50)))
    => (vec (map #(if (== (mod % 2) 0)
                     (double (quot % 2))
                      0.0)
                      (range 0 100)))))

(facts
  "about dense matrix"
  (fact
    "can construct from a seq"
    (let [m (ml/dense-matrix (range 0 16) 8 2)
          rows (.numRows m)
          cols (.numCols m)]
          [rows cols] => [8 2])
    ))

(facts
  "about ")