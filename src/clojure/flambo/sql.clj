;; ## EXPERIMENTAL
;;
;; This code as well as SparkSQL itself are considered experimental.
;;
(ns flambo.sql
  (:require [flambo.api :as f]
            [flambo.interop :as fi]
            [clojure.reflect :as reflect]
            [clojure.set :as set])
  (:import [org.apache.spark.sql.api.java JavaSQLContext Row DataType JavaSchemaRDD StructType StructField]
           [org.apache.spark.sql SQLContext SchemaRDD]
           (org.apache.spark.api.java JavaRDD JavaRDDLike JavaSparkContext)))

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
    (JavaSQLContext. this)))

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
  (^Row row-impl [this]))

(defn row
  "Converts given obj to org.apache.spark.sql.api.java.Row"
  [this]
  (row-impl this))

(defn row-of
  "Creates org.apache.spark.sql.api.java.Row from given elements"
  [& params]
  (Row/create (to-array params)))

(defn vec->row
  "Converts vec to row"
  [v]
  (-> v
      (to-array)
      (Row/create)))

(extend-type java.util.Collection
  PConvertibleToRow
  (^Row row-impl [this]
    (-> this
        (to-array)
        (Row/create))))

(extend-type Row
  PConvertibleToRow
  (^Row row-impl [this]
    (-> this)))

(extend-type flambo.interop.PSeq
  PConvertibleToRow
  (^Row row-impl [this]
    (-> this
        (fi/seq)
        (to-array)
        (Row/create))))

(extend-type Row
  fi/PVec
  (vec-impl [this]
    (row->vec this))
  (nth-impl [this idx]
    (.get this idx)))

(extend-type Row
  fi/PSeq
  (first-impl [this]
    (.get this 0))
  (second-impl [this]
    (.get this 1))
  (seq-impl [this]
    (seq (fi/vec this))))

(defn to-schema-struct
  "creates DataType/StructType for schema description in form [[name datatype]...] [name1 name2...]"
  [name-to-datatype]
  (DataType/createStructType
    (->> name-to-datatype
         (mapv (fn [field-desc]
                 (let [[field-name field-type] (if (vector? field-desc) field-desc [field-desc data-type-binary])]
                   (DataType/createStructField
                     (name field-name)
                     field-type
                     true))
                 )))))

(defn apply-schema
  "Creates a JavaSchemaRDD from an RDD containing Rows by applying a schema to this RDD.
  It is important to make sure that the structure of every Row of the provided RDD matches the provided schema. Otherwise, there will be runtime exception."
  [ssc rdd schema]
  (.applySchema ssc rdd schema))

(defn parallelize-to-schema-rdd
  "Creates a JavaSchemaRDD from given elements."
  [ssc v schema-map]
  (let [sc (f/get-java-spark-context ssc)]
    (apply-schema ssc (f/parallelize sc (map row v)) (to-schema-struct schema-map))))

(extend-type JavaSchemaRDD
  f/PUnionCapableRDD
  (union-impl [rdd rdds]
    (->> (into [rdd] rdds)
         (map (fn [r]
                (.baseSchemaRDD r)))
         (reduce (fn [l r] (.unionAll l r)))
         (.toJavaSchemaRDD))))

(defn map-to-schema-rdd
  "like f/map but takes schema [[name datatype]] and f returning Row"
  [rdd schema-vec f]
  (-> rdd
      (f/map f)
      (apply-schema (get-java-sql-context rdd) (to-schema-struct schema-vec))
      ))

(defn table-schema-rdd
  "returns schema RDD of given table"
  [ssc table-name]
  (.toJavaSchemaRDD (.table (.sqlContext ssc) table-name)))

(def ^:private common-java-rdd-method-names '#{cache coalesce coalesce-max distinct filter intersection persist repartition sample rdd-name subtract union unpersist})

(defn- cols-type [type]
  (if (= type 'org.apache.spark.api.java.JavaRDD)
    "org.apache.spark.api.java.JavaRDDLike"
    type))

(def ^:private java-rdd-methods (->> (:members (reflect/type-reflect JavaRDD))
                                     (filter #(instance? clojure.reflect.Method %1))
                                     (filter #(= (:declaring-class %1) 'org.apache.spark.api.java.JavaRDD))
                                     (filter #(common-java-rdd-method-names (:name %1)))
                                     (map (fn [p] (assoc p :return-type (cols-type (:return-type p))
                                                           :parameter-types (mapv cols-type (:parameter-types p)))))))

(def ^:private java-rdd-like-methods (concat java-rdd-methods
                                             (:members (reflect/type-reflect JavaRDDLike))))

#_(def cols-rdd-methods (into [['getColsMap [] clojure.lang.IPersistentMap]
                              ['getColsVec [] clojure.lang.IPersistentVector]
                              ['getRdd [] org.apache.spark.api.java.JavaRDDLike]]
                             (for [method java-rdd-methods]
                               [(:name method) (:parameter-types method) (:return-type method)])))


(gen-class :name "flambo.sql.ColsRDD"
           :implements [org.apache.spark.api.java.JavaRDDLike]
           :state "state"
           :init "init"
           :constructors {[clojure.lang.IPersistentVector org.apache.spark.api.java.JavaRDDLike] []}
           :methods [[cache [] "org.apache.spark.api.java.JavaRDDLike"]
                     [coalesce [int boolean] "org.apache.spark.api.java.JavaRDDLike"]
                     [coalesce [int] "org.apache.spark.api.java.JavaRDDLike"]
                     [distinct [] "org.apache.spark.api.java.JavaRDDLike"]
                     [distinct [int] "org.apache.spark.api.java.JavaRDDLike"]
                     [filter
                      [org.apache.spark.api.java.function.Function]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [getColsMap [] clojure.lang.IPersistentMap]
                     [getColsVec [] clojure.lang.IPersistentVector]
                     [getRdd [] org.apache.spark.api.java.JavaRDDLike]
                     [intersection
                      ["org.apache.spark.api.java.JavaRDDLike"]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [persist
                      [org.apache.spark.storage.StorageLevel]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [repartition [int] "org.apache.spark.api.java.JavaRDDLike"]
                     [sample [boolean double] "org.apache.spark.api.java.JavaRDDLike"]
                     [sample [boolean double long] "org.apache.spark.api.java.JavaRDDLike"]
                     [subtract
                      ["org.apache.spark.api.java.JavaRDDLike"]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [subtract
                      ["org.apache.spark.api.java.JavaRDDLike"
                       org.apache.spark.Partitioner]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [subtract
                      ["org.apache.spark.api.java.JavaRDDLike" int]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [union
                      ["org.apache.spark.api.java.JavaRDDLike"]
                      "org.apache.spark.api.java.JavaRDDLike"]
                     [unpersist [boolean] "org.apache.spark.api.java.JavaRDDLike"]
                     [unpersist [] "org.apache.spark.api.java.JavaRDDLike"]]
           :prefix "cols-rdd-")

(defn ->cols-rdd
  "Construct RDD wrapper containing information about cols"
  [rdd cols]
  (flambo.sql.ColsRDD. (vec cols) rdd)
  )

(defmacro use-cols-rdd
  "Modify contents of rdd-in with body-form without modifying cols. \n Returns modified cols-rdd.\n Introduces 2 local scope variables: rdd and cols"
  [^flambo.sql.ColsRDD rdd-in & body]
  `(let [~(symbol "rdd") ~rdd-in
         ~(symbol "cols") (.getColsMap ~(symbol "rdd"))]
     (->cols-rdd (do
                   ~@body)
                 (.getColsVec ~(symbol "rdd")))))

(defmacro transform-cols-rdd
  "Modify contents of rdd-in with body-form and replaces current cols of rdd with cols-vec. \n Returns modified cols-rdd.\n Introduces 2 local scope variables: rdd and cols."
  [^flambo.sql.ColsRDD rdd-in cols-vec & body]
  `(let [~(symbol "rdd") ~rdd-in
         ~(symbol "cols") (.getColsMap ~(symbol "rdd"))]
     (->cols-rdd (do
                   ~@body)
                 ~cols-vec)))

(defn cols-rdd-init [cols rdd]
  (let [cols-map (into {} (map-indexed (fn [index val] [val index]) cols))]
    (assert (= (count cols-map) (count cols)))
    [[] {:rdd rdd :cols cols-map}]))

(defmacro ^:private gen-adapter-methods [name overloads]
  (let [this (gensym)]
    `(defn ~(symbol (str "cols-rdd-" name))
       ~@(for [[arity param-types] (group-by count overloads)
               :let [params (map (fn [type] (gensym)) (range arity))]]
           `([~this ~@params]
              (. (:rdd (.state ~this)) ~(symbol name) ~@params))))))

(defmacro ^:private gen-adapter []
  (cons 'list (for [[name overloads] (group-by :name (:members (reflect/type-reflect JavaRDDLike)))]
                `(gen-adapter-methods ~name ~(map :parameter-types overloads)))))

(defmacro ^:private gen-adapter-methods [name overloads]
  (let [this (gensym)]
    `(defn ~(symbol (str "cols-rdd-" name))
       ~@(for [[arity same-arity-overloads] (group-by (fn [overload] (count (:parameter-types overload))) overloads)
               :let [params (map (fn [type] (gensym)) (range arity))
                     is-wrapped (and (common-java-rdd-method-names name)
                                     (= (:return-type (first same-arity-overloads)) 'org.apache.spark.api.java.JavaRDD))]]
           (if is-wrapped
             `([~this ~@params]
                (->cols-rdd (. (:rdd (.state ~this)) ~(symbol name) ~@params) (:cols (.state ~this))))
             `([~this ~@params]
                (. (:rdd (.state ~this)) ~(symbol name) ~@params))
             )))))

(defmacro ^:private gen-adapter []
  (cons 'list (for [[name overloads] (group-by :name java-rdd-like-methods)]
                `(gen-adapter-methods ~name ~overloads))))

(defn cols-map-to-vec [cols-vec]
  (let [col-by-idx (set/map-invert cols-vec)]
    (vec (map col-by-idx (range 0 (count col-by-idx))))))

(defn- cols-rdd-getColsMap [this]
  (:cols (.state this)))

(defn- cols-rdd-getColsVec [this]
  (cols-map-to-vec (:cols (.state this))))

(defn- cols-rdd-getRdd [this]
  (:rdd (.state this)))

(defn- cols-rdd-toString [this]
  (str "Cols: " (.getColsVec this) " wrapped: " (:rdd (.state this))))

(gen-adapter)

(comment                                                    ;example usage
  (->
    (f/parallelize sc (range 1 1000))
    (->cols-rdd ["asd"])
    (use-cols-rdd (f/map rdd (f/fn [row] [row (inc row)]))))

  (->
    (f/parallelize sc (range 1 1000))
    (->cols-rdd ["asd"])
    (transform-cols-rdd ["asd" "qwe"] (f/map rdd (f/fn [row] [(.get (cols "asd") row) (inc row)]))))
  )

(defn to-schema-vec
  "converts StructType to [[name datatype]]"
  [^StructType schema]
  (mapv (fn [^StructField field]
          [(.getName field) (.getDataType field)])
        (.getFields schema)))

(defprotocol PHasColumns
  (^clojure.lang.APersistentMap get-columns-kw-indexes
    [this] "returns map of name_kw-field_index for given schema-rdd")
  (^clojure.lang.APersistentMap get-columns-str-indexes
    [this] "returns map of name-field_index for given schema-rdd")
  (^clojure.lang.APersistentVector get-columns-str-names
    [this] "return names of cols for given table as strings")
  (^clojure.lang.APersistentVector get-columns-kw-names
    [this] "return names of cols for given table as keywords")
  (^clojure.lang.APersistentVector get-schema-vec
    [this] "returns schema-vec in form: [[name datatype]]"))

(extend-type JavaSchemaRDD
  PHasColumns
  (get-columns-kw-indexes [this]
    (into {} (map-indexed
               #(do [(keyword (.getName %2)) %1])
               (->
                 this
                 (.schema)
                 (.getFields)
                 (vec)
                 ))))
  (get-columns-str-indexes [this]
    (into {} (map-indexed
               #(do [(name (.getName %2)) %1])
               (->
                 this
                 (.schema)
                 (.getFields)
                 (vec)
                 ))))
  (get-columns-str-names [this]
    (mapv #(name (.getName %1))
          (->
            this
            (.schema)
            (.getFields)
            )))
  (get-columns-kw-names [this]
    (mapv #(keyword (.getName %1))
          (->
            this
            (.schema)
            (.getFields)
            )))
  (get-schema-vec [this]
    (to-schema-vec (.schema this))))

(extend-type flambo.sql.ColsRDD
  PHasColumns
  (get-columns-kw-indexes [this]
    (into {} (map (fn [[key val]] [(keyword key) val])
                  (.getColsMap this)
                  )))
  (get-columns-str-indexes [this]
    (into {} (map (fn [[key val]] [(name key) val])
                  (.getColsMap this)
                  )))
  (get-columns-str-names [this]
    (mapv name (.getColsVec this)))
  (get-columns-kw-names [this]
    (mapv keyword (.getColsVec this)))
  (get-schema-vec [this]
    (mapv (fn [col-name]
            [col-name data-type-binary])
          (get-columns-str-names this))))

;; ColsRDD specific API

;TODO: impl some ops for cols rdd
;TODO: flat-map-to-cols-rdd, use-cols-indexes, use-cols-idxfns

(defn cols-schema-rdd
  "returns ColsRDD created from SchemaRDD"
  [schema-rdd]
  (->cols-rdd schema-rdd (map keyword (get-columns-str-names schema-rdd))))

(defn parallelize-to-cols-rdd
  "Creates a ColsRDD from given elements"
  [c v cols]
  (let [sc (f/get-java-spark-context c)]
    (->cols-rdd (f/parallelize sc v) cols)))

(defn map-to-cols-rdd
  "like f/map but takes cols [col-name ...] and returns ColsRDD"
  [rdd cols f]
  (-> rdd
      (f/map f)
      (->cols-rdd cols)
      ))

(defmacro with-cols-rdd
  "Macro for convenient access to cols and rdd of ColsRDD."
  [^flambo.sql.ColsRDD rdd-in cols-sym rdd-sym & body]
  `(let [~rdd-sym ~rdd-in
         ~cols-sym (.getColsMap ~rdd-in)]
     (do
       ~@body)))

(defmacro with-cols
  "Macro for convenient access to cols of ColsRDD."
  [^flambo.sql.ColsRDD rdd-in cols-sym & body]
  `(let [~cols-sym (.getColsMap ~rdd-in)]
     (do
       ~@body)))
