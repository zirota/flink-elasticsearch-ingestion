(ns flink-elasticsearch-ingestion.core
  (:require
    [clojure.string :as str]
    [clojure.data.json :as json]
    [io.kosong.flink.clojure.core :as fk])
  (:import (org.apache.flink.api.java.utils ParameterTool)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (java.net URL)
           (org.apache.http HttpHost)
           (org.apache.flink.streaming.connectors.elasticsearch7 ElasticsearchSink$Builder RestClientFactory)
           (org.apache.flink.streaming.connectors.elasticsearch ElasticsearchSinkBase$FlushBackoffType ElasticsearchSinkFunction RequestIndexer)
           (org.elasticsearch.client RestClientBuilder$HttpClientConfigCallback RestClient)
           (org.apache.http.client CredentialsProvider)
           (org.apache.http.auth UsernamePasswordCredentials)
           (org.elasticsearch.action.index IndexRequest)
           (org.elasticsearch.common.xcontent XContentType))
  (:gen-class))

(deftype ElasticsearchHttpClientConfigCallback [username password]
  :load-ns true
  RestClientBuilder$HttpClientConfigCallback
  (customizeHttpClient [this builder]
    (let [provider (reify CredentialsProvider
                     (getCredentials [this scope]
                       (UsernamePasswordCredentials. username password)))]
      (.setDefaultCredentialsProvider builder provider))))

(deftype ElasticsearchRestClientFactory [username password]
  :load-ns true
  RestClientFactory
  (configureRestClientBuilder [this builder]
    (let [callback (->ElasticsearchHttpClientConfigCallback username password)]
      (doto builder
        (.setHttpClientConfigCallback callback)))))

(defn urls->http-hosts [urls]
  (->> (str/split urls #",")
       (map (fn [url-str]
              (let [url (URL. url-str)
                    ^String host (.getHost url)
                    ^String scheme (.getProtocol url)
                    ^int port (if (= (.getPort url) -1)
                                9200
                                (.getPort url))]
                (HttpHost. host port scheme))))
       (reduce conj [])))

(defn ->elasticsearch-client [{:keys [urls username password]}]
  (let [http-hosts (urls->http-hosts urls)
        callback (->ElasticsearchHttpClientConfigCallback username password)]
    (-> (RestClient/builder (into-array HttpHost http-hosts))
        (.setHttpClientConfigCallback callback)
        (.build))))

(defn ->elasticsearch-emitter []
  (reify ElasticsearchSinkFunction
    (process [this record context indexer]
      (let [index-id (:index-id record)
            doc (some-> (:source record)
                        (json/json-str :key-fn name))
            doc-id (:doc-id record)
            index-request (doto (IndexRequest.)
                            (.id doc-id)
                            (.index index-id)
                            (.source doc XContentType/JSON))]
        (.add indexer (into-array IndexRequest [index-request]))))))

(defn ->elasticsearch-sink [http-hosts username password]
  (let [emitter (->elasticsearch-emitter)
        rest-client-factory (->ElasticsearchRestClientFactory username password)
        sink-builder (doto (ElasticsearchSink$Builder. http-hosts emitter)
                       (.setBulkFlushMaxActions 64)
                       (.setBulkFlushInterval 5000)
                       (.setBulkFlushBackoff true)
                       (.setBulkFlushBackoffRetries 10)
                       (.setBulkFlushBackoffDelay 2000)
                       (.setRestClientFactory rest-client-factory)
                       (.setBulkFlushBackoffType ElasticsearchSinkBase$FlushBackoffType/EXPONENTIAL))]
    (.build sink-builder)))

(defn job-graph [env params]
  (let [source-es-username (.get params "source.elasticsearch-username")
        source-es-password (.get params "source.elasticsearch-password")
        source-http-host (urls->http-hosts (.get params "source.elasticsearch-urls"))
        sink-es-username (.get params "sink.elasticsearch-username")
        sink-es-password (.get params "sink.elasticsearch-password")
        sink-http-hosts (urls->http-hosts (.get params "sink.elasticsearch-urls"))
        elasticsearch-sink (->elasticsearch-sink sink-http-hosts sink-es-username sink-es-password)]))

(def default-params
  {"source.elasticsearch-url"      "localhost:9092"
   "source.elasticsearch-username" ""
   "source.elasticsearch-password" ""
   "source.batch-size"             ""
   "source.poll-interval"          ""
   "source.checkpoint-offset"      ""
   "source.poll-interval"          ""
   "sink.elasticsearch-urls"       ""
   "sink.elasticsearch-username"   ""
   "sink.elasticsearch-password"   ""})

(defn -main
  "Flink job for ingesting of data from one elasticsearch index to another."
  [& args]
  (let [args (into-array String args)
        params (-> (ParameterTool/fromMap default-params))
        config (.getConfiguration params)
        env (StreamExecutionEnvironment/getExecutionEnvironment config)]
    (fk/register-clojure-types env)
    (-> env
        (job-graph params)
        (.execute))))


(comment
  (def w (ParameterTool/fromArgs (into-array String ["-hello" "world"])))

  (.get w "hello")
  (.getConfiguration w)

  )


(comment
  ;; TODO: Input configs:
  ;; timestamp field
  ;; batch size
  ;; source elasticsearch
  ;; target elasticsearch

  ;; TODO: Query source Elasticsearch
  ;; use scrolling api
  ;; Check for timestamp field
  ;; If saved timestamp:
  ;;   provide range in query
  ;;   else perform normal query
  ;; Store timestamp of last doc

  ;; TODO: 1 for 1 copy to target elasticsearch index
  )
