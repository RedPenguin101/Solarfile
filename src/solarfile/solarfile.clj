(ns solarfile.solarfile
  (:require [cognitect.aws.client.api :as aws]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [solarfile.domain :refer [process-event]]
            [clj-pgp.keyring :as keyring]))

;; Config - separate eventually

(def config (edn/read-string (slurp (io/resource ".secrets.edn"))))
(def s3 (aws/client {:api :s3}))
#_(aws/validate-requests s3 true)
(def keyring (keyring/load-secret-keyring (slurp (io/resource "keys/privkey.asc"))))

(defonce job-runs (atom {}))

(def file-specs
  {:some-file       {:mask #"trades.csv"
                     :format :csv
                     :instance-identity [{:name :business-date :look-in :file-name :pattern #"\d{4}-\d{2}-\d{2}"}
                                         {:name :bd-in-file :look-in :file-content :fn (fn [_] :not-implemented)}]
                     :expectations []
                     :endpoints []}
   :some-other-file {:mask #"test.txt"
                     :format :txt
                     :instance-identity [{:name "Test" :look-in :constant :value "Test"}]
                     :expectations []
                     :endpoints []}
   :encrypted-file  {:mask #"encrypt.txt.pgp"
                     :format :txt
                     :instance-identity [{:name :test :look-in :constant :value "Test"}]
                     :decryption {:key-loc "resources/keys/privkey.asc"
                                  :password "welcome"}
                     :expectations []
                     :endpoints []}
   :portfolio       {:mask #"portfolios.csv"
                     :format :csv
                     :instance-identity [{:name :business-date
                                          :look-in :file-content-csv
                                          :cell [4 2]}]}
   :doesnt-exist    {:mask #"doesntexist.txt"
                     :format :txt
                     :instance-identity [{:name "Test" :look-in :constant :value "Test"}]
                     :expectations []
                     :endpoints []}})

(defn- read-body-stream [file] (update file :Body slurp))

(defn- get-and-check-s3
  "Given an S3 bucket and a key, trys to fetch the object. Throws if the response
   contains an error."
  [bucket key]
  (let [response (aws/invoke s3 {:op :GetObject :request {:Bucket bucket :Key key}})]
    (if (:Error response)
      (throw java.io.FileNotFoundException)
      response)))

(defn- get-file! [file-key protocol]
  (case protocol
    :s3    (read-body-stream (assoc (get-and-check-s3 (-> config :s3 :bucket-name) file-key) :file-name file-key :source :s3))
    :local {:Body (slurp (str "resources/" file-key)) :file-name file-key :source :local}))

(defn get-keyring [key-loc]
  (keyring/load-secret-keyring (slurp key-loc)))

(defn process-file-event! [event]
  (process-event event file-specs get-file! job-runs keyring))

(comment
  (defn uuid [] (java.util.UUID/randomUUID))

  (process-file-event! {:file-name "test.txt" :location :local})

  (process-file-event! {:file-name "notinterested.txt" :location :local
                        :run-id (uuid)})
  (process-file-event! {:file-name "2021-05-21_trades.csv" :location :local})
  (process-file-event! {:file-name "portfolios.csv" :location :local
                        :run-id (uuid)})
  (process-file-event! {:file-name "encrypt.txt.pgp"
                        :location :local
                        :run-id (uuid)})
  (process-file-event! {:file-name "doesntexist.txt"
                        :location :local
                        :run-id (uuid)})

  (process-file-event! {:file-name "test.txt" :location :embedded
                        :data "hello world"
                        :run-id (uuid)})
  (process-file-event! {:file-name "test.txt"
                        :location :embedded
                        :data "hello world"
                        :run-id (uuid)
                        :dry-run true})

  @job-runs
  (reset! job-runs {})

  (process-file-event! {:file-name "encrypt.txt.pgp" :location :s3})

  (map #(process-file-event! {:file-name % :location :local})
       ["encrypt.txt.pgp" "test.txt" "notinterested.txt" "2021-05-21_trades.csv"])

  1)
