(defproject com.tangramcare/flambo "0.4.0-SNAPSHOT"
  :description "A Clojure DSL for Apache Spark"
  :url "http://git.itx.pl/o2/flambofork/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.tangramcare/serializable-fn "0.0.6-SNAPSHOT"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/carbonite "1.4.0"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/chill_2.10 "0.5.0"
                  :exclusions [org.scala-lang/scala-library]]]
  :repositories [["snapshots" {:url      "http://exotica.itx.pl:8080/nexus-2.8.0-05/content/groups/public"
                               :update   :always
                               :checksum :fail}]]
  :deploy-repositories [["snapshots" "http://exotica.itx.pl:8080/nexus-2.8.0-05/content/repositories/snapshots/"]
                        ["releases" "http://exotica.itx.pl:8080/nexus-2.8.0-05/repositories/releases/"]]

  :aot [flambo.function
        flambo.api
        flambo.sql
        flambo.kryo.sfn-serializer
        flambo.example.tfidf
        ]
  :profiles {:dev
             {:dependencies [[midje "1.6.3"]
                             [criterium "0.4.3"]]
              :plugins [[lein-midje "3.1.3"]
                        [lein-marginalia "0.8.0"]
                        [codox "0.8.9"]]}
             :provided
             {:dependencies
              [[org.apache.spark/spark-core_2.10 "1.2.0"]
               [org.apache.spark/spark-streaming_2.10 "1.2.0"]
               [org.apache.spark/spark-streaming-kafka_2.10 "1.2.0"]
               [org.apache.spark/spark-streaming-flume_2.10 "1.2.0"]
               [org.apache.spark/spark-sql_2.10 "1.2.0"]
               [org.apache.spark/spark-mllib_2.10 "1.2.0"]]}
             :uberjar
             {:aot :all}}
  :checksum :warn ;https://issues.apache.org/jira/browse/SPARK-5308
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :codox {:defaults {:doc/format :markdown}
          :include [flambo.api flambo.conf flambo.kryo]
          :output-dir "doc/codox"
          :src-dir-uri "http://github.com/yieldbot/flambo/blob/develop/"
          :src-linenum-anchor-prefix "L"}
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ^:replace ["-server" "-Xmx1g"]
  :global-vars {*warn-on-reflection* false}
  :min-lein-version "2.5.0")
