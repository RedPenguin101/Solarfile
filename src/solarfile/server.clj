(ns solarfile.server
  (:require [ring.adapter.jetty :as jetty]
            [clojure.pprint :as pprint]
            [solarfile.solarfile :refer [process-file-event!]]
            [compojure.core :as comp :refer [defroutes]]
            [compojure.route :as route]
            [clojure.data.json :as json]))

(defn- write-edn-as-response [response]
  (with-out-str (pprint/pprint response)))

(def json-with-kw #(json/read-str % :key-fn keyword))

(defn event-coerce [req-event]
  (-> req-event
      (update :location keyword)))

(defn response-coerce [response]
  (json/write-str (update-in response [:file-spec :mask] str)))

(defn file-event-handler [req]
  {:status 200
   :body (-> req :body slurp json-with-kw event-coerce process-file-event! response-coerce)
   :headers {"Content-Type" "text/plain"}})

(defroutes app
  (comp/GET "/" [] {:status 200
                    :body (json/write-str {"Hello" "World" "post" "klsdfg"})
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