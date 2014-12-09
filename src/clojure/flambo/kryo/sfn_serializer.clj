(ns flambo.kryo.sfn-serializer
  (:require
    [serializable.fn :as sfn]
    [flambo.kryo :as fkryo]
    )
  (:gen-class
    :name flambo.kryo.SfnSerializer
    :extends com.esotericsoftware.kryo.Serializer
  ))

(defn -write [this kryo output object]
  (binding [sfn/*serialize* fkryo/serialize]
    (let [buffer (sfn/serialize object)]
      (.writeInt output (alength buffer))
      (.write output buffer))))

(defn -read [this kryo input cls]
  (binding [sfn/*deserialize* fkryo/deserialize]
    (sfn/deserialize (.readBytes input (.readInt input)))))

(defn register [kryo]
  (.register kryo clojure.lang.AFunction$1 (flambo.kryo.SfnSerializer.)))
