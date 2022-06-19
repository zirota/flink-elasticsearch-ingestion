(ns flink-elasticsearch-ingestion.core
  (:require
    [clojure.string :as str]
    [clojure.data.json :as json]
    [clj-http.client :as client]
    [io.kosong.flink.clojure.core :as fk])
  (:import (org.apache.flink.api.java.utils ParameterTool)
           (org.apache.flink.streaming.api.environment StreamExecutionEnvironment)
           (java.net URL)
           (java.time Instant)
           (org.apache.http HttpHost)
           (org.apache.flink.streaming.connectors.elasticsearch7 ElasticsearchSink$Builder RestClientFactory)
           (org.apache.flink.streaming.connectors.elasticsearch ElasticsearchSinkBase$FlushBackoffType ElasticsearchSinkFunction RequestIndexer)
           (org.elasticsearch.client RestClientBuilder$HttpClientConfigCallback RestClient)
           (org.apache.http.client CredentialsProvider)
           (org.apache.http.auth UsernamePasswordCredentials)
           (org.elasticsearch.action.index IndexRequest)
           (org.elasticsearch.common.xcontent XContentType)
           (org.apache.flink.api.common.state ListStateDescriptor ListState)
           (org.apache.flink.api.common.typeinfo Types)
           (java.util UUID))
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

(defn- fetch-documents-with-scroll-id!
  "Fetch documents from elasticsearch using an existing scroll-id."
  [url auth body]
  (json/read-str (:body (client/request {:method       :post
                                         :basic-auth   auth
                                         :url          (str url "/_search/scroll")
                                         :content-type :json
                                         :body         (json/write-str body)}))))

(defn- fetch-documents-with-sliced-scroll!
  "Fetch documents from elasticsearch by performing a scrolling search."
  [url auth index query-params body]
  (json/read-str (:body (client/request {:method       :get
                                         :basic-auth   auth
                                         :url          (str url "/" index "/_search")
                                         :content-type :json
                                         :query-params query-params
                                         :body         (json/write-str body)}))))

(defn fetch-elasticsearch-documents! [{:keys [url
                                              index
                                              batch-size
                                              username
                                              password
                                              timestamp
                                              scroll-id]}]
  (let [auth (when (and username
                        password) [username password])
        scroll-context {"scroll" "1m"}
        slice-context {"size"  batch-size
                       "slice" {"field" "@timestamp"
                                "id"    0}}
        query (if timestamp
                {"range" {"@timestamp" {"gt" timestamp}}}
                {"match_all" {}})
        query-body (merge-with slice-context {"query" query})]
    (if scroll-id
      (fetch-documents-with-scroll-id! url auth (merge-with scroll-context {"scroll-id" scroll-id}))
      (fetch-documents-with-sliced-scroll! url auth index scroll-context query-body))))

(defn elasticsearch-source-open [this _]
  (let [state (.state this)
        params (ParameterTool/fromMap (some-> this
                                              .getRuntimeContext
                                              .getExecutionConfig
                                              .getGlobalJobParameters
                                              .toMap))
        url (.get params "source.elasticsearch-url")
        index (.get params "source.index")
        batch-size (or (.getInt params "source.batch-size") 1000)
        username (.get params "source.elasticsearch-username")
        password (.get params "source.elasticsearch-password")
        poll-interval (or (.getLong params "source.poll-interval") 5000)]
    (swap! state assoc :es-source-url url)
    (swap! state assoc :es-source-index index)
    (swap! state assoc :es-source-batch-size batch-size)
    (swap! state assoc :es-source-username username)
    (swap! state assoc :es-source-password password)
    (swap! state assoc :es-source-poll-interval poll-interval)))

(defn elasticsearch-source-init-state [this context]
  (let [state (.state this)
        checkpoint-state-descriptor (ListStateDescriptor. "last-updated" Types/LONG)
        checkpoint-state (-> context
                             (.getOperatorStateStore)
                             (.getListState checkpoint-state-descriptor))]
    (swap! state assoc :checkpoint-state checkpoint-state)
    (when-not (some-> checkpoint-state .get first)
      (.update checkpoint-state [0]))
    (if (.isRestored context)
      (let [last-update-time (or (some-> checkpoint-state .get first) 0)]
        (swap! state assoc :last-update-time last-update-time))
      (swap! state assoc :last-update-time 0))))

(defn elasticsearch-source-snapshot-state [this _]
  (let [state (.state this)
        last-update-time (:last-update-time @state)
        ^ListState checkpoint-state (:checkpoint-state @state)]
    (.clear checkpoint-state)
    (.add checkpoint-state last-update-time)))

(defn is-before-timestamp?
  "Returns true if timestamp-a is before (<) timestamp-b"
  [timestamp-a timestamp-b]
  (let [instant-a (Instant/parse timestamp-a)
        instant-b (Instant/parse timestamp-b)]
    (.isBefore instant-a instant-b)))

(defn elasticsearch-source-run [this context]
  (let [state (.state this)
        poll-interval (:es-source-poll-interval state)]
    (swap! state assoc :running true)
    (while (:running @state)
      (let [prev-update-time (:last-update-time @state)
            result (fetch-elasticsearch-documents! {:url        (:es-source-url @state)
                                                    :index      (:es-source-index @state)
                                                    :batch-size (:es-source-batch-size @state)
                                                    :username   (:es-source-username @state)
                                                    :password   (:es-source-password @state)
                                                    :timestamp  prev-update-time
                                                    :scroll-id  (:es-source-scroll-id @state)})
            scroll-id (get result "_scroll_id")
            document-hits (get-in result ["hits" "hits"])]
        (doseq [doc document-hits]
          (let [source (get doc "_source")
                current-update-time (get source "@timestamp")
                index-id (get doc "_index")
                doc-id (get doc "_id")]
            (when (is-before-timestamp? prev-update-time current-update-time)
              (.collect context {:index-id index-id
                                 :doc-id   doc-id
                                 :source   source})
              (swap! state assoc
                     :es-source-scroll-id scroll-id
                     :last-update-time current-update-time)))))
      (Thread/sleep poll-interval))))

(defn elasticsearch-source-cancel [this]
  (let [state (.state this)]
    (swap! state assoc :running false)))

;; RichSourceFunction
(def elasticsearch-source
  (fk/flink-fn
    {:fn              :source
     :returns         (fk/type-info-of {})
     :init            (fn [_] (atom {}))
     :open            elasticsearch-source-open
     :run             elasticsearch-source-run
     :cancel          elasticsearch-source-cancel
     :initializeState elasticsearch-source-init-state
     :snapshotState   elasticsearch-source-snapshot-state}))

(def document-id-selector
  (fk/flink-fn
    {:fn :key-selector
     :returns Types/STRING
     :getKey (fn [_ value]
               (:id value))}))

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
  (let [sink-es-username (.get params "sink.elasticsearch-username")
        sink-es-password (.get params "sink.elasticsearch-password")
        sink-http-hosts (urls->http-hosts (.get params "sink.elasticsearch-urls"))
        elasticsearch-sink (->elasticsearch-sink sink-http-hosts sink-es-username sink-es-password)]
    (-> env
        (.addSource elasticsearch-source)
        (.uid (str (UUID/randomUUID)))
        (.name "Elasticsearch Index Source")
        (.keyBy document-id-selector)
        (.print)))
        ;(.uid (str (UUID/randomUUID)))
        ;(.addSink elasticsearch-sink)
        ;(.uid (str (UUID/randomUUID)))
        ;(.name "Elasticsearch Sink"))
        ;)
  env)

(def default-params
  {"source.elasticsearch-url"      "http://localhost:9092"
   "source.elasticsearch-username" ""
   "source.elasticsearch-password" ""
   "source.index"                  "kibana_sample_data_logs"
   "source.batch-size"             "1000"
   "source.poll-interval"          "5000"
   "sink.elasticsearch-urls"       "http://localhost:9092"
   "sink.elasticsearch-username"   ""
   "sink.elasticsearch-password"   ""})

(defn -main
  "Flink job for ingesting of data from one Elasticsearch index to another."
  [& args]
  (let [args (into-array String args)
        params (-> (ParameterTool/fromMap default-params)
                   (.mergeWith (ParameterTool/fromArgs args)))
        config (.getConfiguration params)
        env (StreamExecutionEnvironment/getExecutionEnvironment config)]
    (.. env getConfig (setGlobalJobParameters params))
    (fk/register-clojure-types env)
    (-> env
        (job-graph params)
        (.execute))))

(comment
  (-main)
  )
