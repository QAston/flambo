(ns flambo.sql-test
  (:use midje.sweet)
  (:require [flambo.api :as f]
            [flambo.sql :as sql]
            [flambo.conf :as conf])
  (:import (org.apache.spark.sql.api.java JavaSQLContext JavaSchemaRDD StructType)))

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
  "about schema-rdd"

  (let [conf (-> (conf/spark-conf)
                 (conf/master "local[*]")
                 (conf/app-name "sql-test"))]
    (f/with-context c conf
      (let [sc (sql/sql-context c)]
        (fact
          "create-schema us a schema object"
          (class (sql/create-schema {:num1 sql/data-type-integer
                          :num2 sql/data-type-integer
                          :str1 sql/data-type-string})) => StructType)

        (fact
          "parallelize-to-schema-rdd gives us a JavaSchemaRDD"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                           :num2 sql/data-type-integer
                                                                           :str1 sql/data-type-string})
            (class)) => JavaSchemaRDD)

        (fact
          "union of schema rdds returns schema rdd"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                           :num2 sql/data-type-integer
                                                                           :str1 sql/data-type-string})

              (f/union (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                                    :num2 sql/data-type-integer
                                                                                    :str1 sql/data-type-string})
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                                    :num2 sql/data-type-integer
                                                                                    :str1 sql/data-type-string})
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                                    :num2 sql/data-type-integer
                                                                                    :str1 sql/data-type-string}))
              (class)) => JavaSchemaRDD)

        (fact
          "union of schema rdds returns concatenated schema rdd"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                           :num2 sql/data-type-integer
                                                                           :str1 sql/data-type-string})

              (f/union (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                                    :num2 sql/data-type-integer
                                                                                    :str1 sql/data-type-string})
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                                    :num2 sql/data-type-integer
                                                                                    :str1 sql/data-type-string})
                       (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                                    :num2 sql/data-type-integer
                                                                                    :str1 sql/data-type-string}))
              (f/count)) => 8)

        (fact
          "union of 2 schema rdds returns schema rdd"
          (-> (sql/parallelize-to-schema-rdd sc [[1 2 "asd"] [4 5 "qwe"]] {:num1 sql/data-type-integer
                                                                           :num2 sql/data-type-integer
                                                                           :str1 sql/data-type-string})
              (class)) => JavaSchemaRDD)))))