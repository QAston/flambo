;; ## EXPERIMENTAL
;;
;; This code as well as SparkSQL itself are considered experimental.
;;
(ns flambo.sql
  (:require [flambo.api :as f])
  (:import [org.apache.spark.sql.api.java JavaSQLContext Row DataType JavaSchemaRDD]
           [org.apache.spark.sql SQLContext SchemaRDD]
           (org.apache.spark.api.java JavaRDDLike JavaSparkContext)))

;; data types

(def data-type-string DataType/StringType)
(def data-type-binary DataType/BinaryType)
(def data-type-timestamp DataType/TimestampType)
(def data-type-double DataType/DoubleType)
(def data-type-float DataType/FloatType)
(def data-type-byte DataType/ByteType)
(def data-type-integer DataType/IntegerType)
(def data-type-long DataType/LongType)
(def data-type-short DataType/ShortType)
(def data-type-null DataType/NullType)

;; ## JavaSQLContext
;;
(defn sql-context [spark-context]
  (JavaSQLContext. spark-context))

(defn sql [sql-context query]
  (.sql sql-context query))

(defn parquet-file [sql-context path]
  (.parquetFile sql-context path))

(defn json-file [sql-context path]
  (.jsonFile sql-context path))

(defn register-rdd-as-table [sql-context rdd table-name]
  (.registerRDDAsTable sql-context rdd table-name))

(defn cache-table [sql-context table-name]
  (let [scala-sql-context (.sqlContext sql-context)]
    (.cacheTable scala-sql-context table-name)))

;; ## JavaSchemaRDD
;;
(defn register-as-table [rdd table-name]
  (.registerAsTable rdd table-name))

(extend-type SQLContext
  f/PHasContext
  (^JavaSparkContext get-java-spark-context [this]
    (f/get-java-spark-context (.sparkContext this))))

(extend-type JavaSQLContext
  f/PHasContext
  (^JavaSparkContext get-java-spark-context [this]
    (f/get-java-spark-context (.sqlContext this))))

(defprotocol PHasSQLContext
  "Types capable of receiving spark context"
  (^JavaSQLContext get-java-sql-context [this]))

(extend-type SQLContext
  PHasSQLContext
  (^JavaSQLContext get-java-sql-context [this]
    (JavaSQLContext. (.sqlContext this))))

(extend-type SchemaRDD
  PHasSQLContext
  (^JavaSQLContext get-java-sql-context [this]
    (get-java-sql-context (.sqlContext this))))

(extend-type JavaSchemaRDD
  PHasSQLContext
  (^JavaSQLContext get-java-sql-context [this]
    (get-java-sql-context (.schemaRDD this))))

(def print-schema (memfn printSchema))

;; ## Row
;;
(defn row->vec [^Row row]
  (let [n (.length row)]
    (loop [i 0 v (transient [])]
      (if (< i n)
        (recur (inc i) (conj! v (.get row i)))
        (persistent! v)))))

(defprotocol PConvertibleToRow
  (^Row to-row [this]))

(extend-type java.util.Collection
  PConvertibleToRow
  (^Row to-row [this]
    (-> this
        (to-array)
        (Row/create))))

(extend-type Row
  PConvertibleToRow
  (^Row to-row [this]
    this))

(defn create-schema
  "creates schema for name-datatype map"
  [name-to-datatype]
  (DataType/createStructType
    (vec (->> name-to-datatype
              (map (fn [[n datatype]]
                     (DataType/createStructField
                       (name n)
                       datatype
                       true)))))))

(defn apply-schema
  "Creates a JavaSchemaRDD from an RDD containing Rows by applying a schema to this RDD.
  It is important to make sure that the structure of every Row of the provided RDD matches the provided schema. Otherwise, there will be runtime exception."
  [ssc rdd schema]
  (.applySchema ssc rdd schema))

(defn parallelize-to-schema-rdd
  "Creates a JavaSchemaRDD from an RDD containing Rows by applying a schema to this RDD.
  It is important to make sure that the structure of every Row of the provided RDD matches the provided schema. Otherwise, there will be runtime exception."
  [ssc v schema-map]
  (let [sc (f/get-java-spark-context ssc)]
        (apply-schema ssc (f/parallelize sc (map to-row v)) (create-schema schema-map))))

(extend-type JavaSchemaRDD
  f/PUnionCapableRDD
  (union-impl [rdd rdds]
    (->> (into [rdd] rdds)
         (map (fn [r]
                (.baseSchemaRDD r)))
         (reduce (fn [l r] (.unionAll l r)))
         (.toJavaSchemaRDD))))
