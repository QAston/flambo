(ns flambo.kryoserializer
  (:import [com.esotericsoftware.kryo Kryo]
           [org.apache.spark.serializer KryoRegistrator])
  (:require [carbonite.buffer :as buffer]))

(defn kryo-serializer [sc & {:keys [^KryoRegistrator registrator] :or {registrator (flambo.serialize.BaseFlamboRegistrator.)}}]
  (let [kryo (Kryo.)]
    (.registerClasses registrator kryo)
    kryo))

(defn round-trip [registry o]
  (->> o
       (buffer/write-bytes registry)
       (buffer/read-bytes registry)))