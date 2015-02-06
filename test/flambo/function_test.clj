(ns flambo.function-test
  (:require [flambo.api :as f]
            [flambo.kryoserializer :as ks]
            [clojure.test :refer :all]
            [flambo.conf :as conf]
            [flambo.function :as ff]
            [flambo.kryo :as kryo]))


(deftest serializable-functions

  (f/with-context sc (-> (conf/spark-conf)
                         (conf/master "local[*]")
                         (conf/app-name "api-test"))
    (let [kryo (ks/kryo-serializer sc)
          myfn (f/fn [x] (* 2 x))]

      (testing
        "we can serialize and deserialize it to a function"
        (is (fn? (ks/round-trip kryo myfn))))

      (testing (is (= 6
                      ((ks/round-trip kryo myfn) 3))))


      (let [x {:f identity :g (f/fn [s] (identity s))}]     ;; check discussion on https://github.com/yieldbot/flambo/issues/42
        (testing (is (= 'test-symbol
                        (((ks/round-trip kryo x) :g)
                          'test-symbol)
                        )))))))
