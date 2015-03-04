(ns flambo.sql-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.sql :as sql]
            [flambo.conf :as conf]
            [flambo.interop :as fi])
  (:import (org.apache.spark.sql.api.java JavaSQLContext JavaSchemaRDD StructType)
           (flambo.sql ColsRDD)))

(facts
  "about sql-context"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "sql-test"))]
    (f/with-context c conf
      (let [sc (sql/sql-context c)]
        (fact
          "gives us a JavaSqlContext"
          (class sc) => JavaSQLContext)))))

(facts
  "about interop - row"
  (let [t (sql/row-of "a" "b")]
    (fact "first works"
          (fi/first t) => "a")
    (fact "second works"
          (fi/second t) => "b")
    (fact "seq works"
          (fi/seq t) => (list "a" "b"))
    (fact "vec works"
          (fi/vec t) => (vector "a" "b"))
    (fact "nth works"
          (fi/nth t 0) => "a")
    (fact "nth works"
          (fi/nth t 1) => "b")))

(facts
  "about schema-rdd"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "sql-test"))
        schema-vec [[:num1 sql/data-type-integer]
                    [:num2 sql/data-type-integer]
                    [:str1 sql/data-type-string]]
        schema-vec-names [:num1
                    :num2
                    :str1]]
    (f/with-context c conf
      (let [sc (sql/sql-context c)]
        (fact
          "create-schema creates us a schema object"
          (class (sql/to-schema-struct  schema-vec)) => StructType)

        (fact
          "create-schema creates us a schema object"
          (class (sql/to-schema-struct  schema-vec-names)) => StructType)

        (fact
          "parallelize-to-schema-rdd gives us a JavaSchemaRDD"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)
            (class)) => JavaSchemaRDD)

        (fact
          "union of schema rdds returns schema rdd"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)

              (f/union (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec))
              (class)) => JavaSchemaRDD)

        (fact
          "union of schema rdds returns concatenated schema rdd"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)

              (f/union (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec))
              (f/count)) => 8)

        (fact
          "union of 2 schema rdds returns schema rdd"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] schema-vec)
              (class)) => JavaSchemaRDD)))))

(facts
  "about cols-rdd"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "sql-test"))
        default-cols [:num1
                    :num2
                    :str1]]
    (f/with-context c conf
                    (let [sc (sql/sql-context c)]

                      (fact
                        "parallelize-to-cols-rdd gives us a ColsRDD"
                        (-> (sql/parallelize-to-cols-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                            (class)) => ColsRDD)

                      (fact
                        "cache persists this RDD with a default storage level (MEMORY_ONLY)"
                        (-> (sql/parallelize-to-cols-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                            (f/cache)
                            (f/collect)) => [[1 2 "asd"] [4 5 "qwe"]])

                      (fact
                        "filter returns an RDD formed by selecting those elements of the source on which func returns true"
                        (-> (sql/parallelize-to-cols-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                            (f/filter (f/fn [x] (even? (first x))))
                            f/collect
                            vec) => [[4 5 "qwe"]])

                      #_(fact
                        "union of schema rdds returns schema rdd"
                        (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)

                            (f/union (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                                     (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                                     (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols))
                            (class)) => JavaSchemaRDD)

                      #_(fact
                        "union of schema rdds returns concatenated schema rdd"
                        (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)

                            (f/union (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                                     (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                                     (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols))
                            (f/count)) => 8)

                      #_(fact
                        "union of 2 schema rdds returns schema rdd"
                        (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] default-cols)
                            (class)) => JavaSchemaRDD)))))