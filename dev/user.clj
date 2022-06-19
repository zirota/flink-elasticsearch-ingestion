(ns user
  (:require [clojure.data.json :as json]
            [clj-http.client :as client]))

(comment
  (def url "http://localhost:9200")
  (def endpoint (str url "/" "kibana_sample_data_logs" "/_search"))

  (def get-request-body
    {"slice" {"field" "@timestamp"
              "id"    0}
     "query" {"range" {"timestamp" {"gte" "2022-06-05T05:36:25.812Z"}}}})

  (def get-result (json/read-str (:body (client/request {:method       :get
                                                         :url          endpoint
                                                         :content-type :json
                                                         :query-params {"scroll" "1m"}
                                                         :body         (json/write-str get-request-body)}))))

  (def scroll-id (get get-result "_scroll_id"))

  (def document-hits (get-in get-result ["hits" "hits"]))

  (def last-doc (last document-hits))

  (get-in last-doc ["_source" "timestamp"])



  (def post-request-body {"scroll"    "1m"
                          "scroll_id" "FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFjR2aXRwdVM5U0hXRVBPdDFlcTVHNHcAAAAAAAAJ6xZMSEN4dzNHTFJjNkN5QVpHMXQ3NVBn"})

  (def post-result (json/read-str (:body (client/request {:method       :post
                                                          :url          (str url "/_search/scroll")
                                                          :content-type :json
                                                          :body         (json/write-str post-request-body)}))))
  )