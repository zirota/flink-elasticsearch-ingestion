(def version {:elasticsearch "7.17.1"
              :flink         "1.14.5"
              :log4j         "2.17.2"})

(defproject flink-elasticsearch-ingestion "0.1.0-SNAPSHOT"
  :description "Flink ingestion job for indexing data from one Elasticsearch cluster to another"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.json "2.4.0"]
                 [org.clojure/tools.logging "1.2.4"]
                 [clj-http "3.12.3"]
                 [org.apache.logging.log4j/log4j-api ~(:log4j version)]
                 [org.apache.logging.log4j/log4j-core ~(:log4j version)]
                 [io.kosong.flink/flink-clojure "0.1.0"]
                 [org.apache.flink/flink-connector-elasticsearch7_2.12 ~(:flink version)]
                 [org.apache.flink/flink-streaming-java_2.12 ~(:flink version) :scope "provided"]
                 [org.apache.flink/flink-runtime-web_2.12 ~(:flink version) :scope "provided"]
                 [org.elasticsearch.client/elasticsearch-rest-client ~(:elasticsearch version)]]
  
  :main flink-elasticsearch-ingestion.core
  :aot [flink-elasticsearch-ingestion.core]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
