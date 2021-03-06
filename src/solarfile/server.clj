(ns solarfile.server
  (:require [ring.adapter.jetty :as jetty]
            [clojure.pprint :as pprint]
            [solarfile.solarfile :refer [process-file-event!] :as sf]
            [compojure.core :as comp :refer [defroutes]]
            [clojure.data.json :as json]))

(defn- write-edn-as-response [response]
  (with-out-str (pprint/pprint response)))

(defn update-vals [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn filter-vals [f m]
  (into {} (filter (fn [[k v]] (f v)) m)))

(def json-with-kw #(json/read-str % :key-fn keyword))

(defn event-coerce [req-event]
  (-> req-event
      (update :location keyword)))

(defn response-coerce [response]
  (update-in response [:file-spec :mask] str))

(defn file-event-handler [req]
  {:status 200
   :body (-> req :body slurp json-with-kw event-coerce process-file-event! response-coerce json/write-str)
   :headers {"Content-Type" "application/json"}})

(defroutes app
  (comp/GET "/" [] {:status 200
                    :body (json/write-str {"Hello" "World"})
                    :headers {"Content-Type" "application/json"}})
  (comp/GET "/health" [] {:status 200
                          :body (json/write-str {"status" "available"})
                          :headers {"Content-Type" "application/json"}})
  (comp/GET "/job-runs" [] {:status 200
                            :body (json/write-str (update-vals response-coerce @sf/job-runs))
                            :headers {"Content-Type" "application/json"}})
  (comp/GET "/failed-job-runs" [] {:status 200
                                   :body (json/write-str (update-vals response-coerce (filter-vals #(= :failed (:job-status %)) @sf/job-runs)))
                                   :headers {"Content-Type" "application/json"}})
  (comp/POST "/event" req (file-event-handler req)))

(defonce server (atom nil))

(defn start []
  (reset! server (jetty/run-jetty (fn [req] (app req)) {:port 3000 :join? false})))

(defn stop []
  (when-some [s @server]
    (.stop s)
    (reset! server nil)))

(comment
  (start)
  (stop))