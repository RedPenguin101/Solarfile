(ns solarfile.solarfile
  (:require [cognitect.aws.client.api :as aws]
            [clojure.data.csv :as csv]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clj-pgp.core :as pgp]
            [clj-pgp.keyring :as keyring]
            [clj-pgp.message :as pgp-msg]))

;; Config - separate eventually

(def config (edn/read-string (slurp (io/resource ".secrets.edn"))))
(def s3 (aws/client {:api :s3}))
#_(aws/validate-requests s3 true)

(defonce job-runs (atom {}))

(defn- uuid [] (java.util.UUID/randomUUID))

;; Getting files

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

(comment
  "objects come back from S3 like this:"

  {:LastModified #inst "2021-05-22T09:57:13.000-00:00"
   :ETag "\"098f6bcd4621d373cade4e832627b4f6\""
   :Metadata {}
   :ContentLength 4
   :ContentType "text/plain"
   :AcceptRanges "bytes"
   :Body "test"})

;; File Specs

(comment
  "File Specs have:
   * masks (required) - this is to check the identity of the file spec
                        and decide whether to download the file (on a hit)
   * instance identity (required) - this is how you identify the instance of the
     file - what the effective date is, what the fund is, etc. It takes the form
     of an array of records with rules on how to determine a particular identity
     attribute (see establish identity section for details)")

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

(defn- find-file-spec [filename specs]
  (let [spec-matches (keep (fn [[spec spec-def]] (when (re-find (:mask spec-def) filename) spec)) specs)]
    (cond
      (= (count spec-matches) 1) (assoc ((first spec-matches) specs) :spec-name (first spec-matches))
      (> (count spec-matches) 1) {:errors [["Multiple filespec matches" spec-matches]]}
      (zero? (count spec-matches)) nil)))

(comment
  (find-file-spec "anothertest.txt" file-specs)
  (find-file-spec "test.txt" file-specs)
  (find-file-spec "nomatch.txt" file-specs)
  1)

;; Establish identity

(defn- read-csv-cell [file [row col]]
  (get-in (vec (csv/read-csv file)) [(dec row) (dec col)]))

(defn- establish-identity [file rule]
  (case (:look-in rule)
    :file-name (if-let [value (re-find (:pattern rule) (:file-name file))]
                 {(:name rule) value}
                 (throw (ex-info "Couldn't establish identity" {:rule rule :file-name (:file-name file)})))
    :file-content-csv {(:name rule) (read-csv-cell (:Body file) (:cell rule))}
    :file-content {(:name rule) ((:fn rule) (:Body file))}
    :constant {(:name rule) (:value rule)}))

(comment
  (establish-identity {:file-name "2021-01-01_test.txt"}
                      {:name :business-date
                       :look-in :file-name
                       :pattern #"\d{4}-\d{2}-\d{2}"})

  (establish-identity {:file-name "2021-01-01_test.txt"}
                      {:name :bd-in-file
                       :look-in
                       :file-content
                       :fn (fn [_] :not-implemented)}))

;; Decryption

(defn- decrypt [key-loc password message]
  (let [keyring (keyring/load-secret-keyring (slurp key-loc))]
    (pgp-msg/decrypt message (pgp/unlock-key (keyring/get-secret-key keyring (first (keyring/list-public-keys keyring))) password))))

(comment
  (decrypt "resources/keys/privkey.asc" "welcome" (slurp "resources/encrypt.txt.pgp")))

;;;;;;;;;;;;;;;;; Pipeline functions and pipe ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pipelines operate on "flocks" of data - maps, flowing them through the 
;; pipeline and accreting new data to them through each pipe section
;; Pipe functions just wrap some business functionality so it fits the 
;; pipeline pattern: 
;;                      function :: flock -> flock
;; usually by assoc'ing in a new key. 
;; They usually accrete the pipe-logs at the same time
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- now [] (new java.util.Date))

(defn- event-check [event]
  (if (every? #(contains? event %) [:file-name :location :run-id])
    event
    (throw (ex-info "Event is missing required keys" {:event event
                                                      :errors [{:error-message "Event is missing required keys"
                                                                :data event}]
                                                      :job-status :failed
                                                      :run-id (java.util.UUID/randomUUID)}))))

(defn- pipe-prep [event]
  (assoc (merge (select-keys (event-check event) [:file-name :location :run-id])
                {:event event :logs []})
         :process-start (now)
         :job-status :in-progress))

(defn- pipe-file-spec [flock]
  (if-let [spec (find-file-spec (:file-name flock) file-specs)]
    (assoc flock :file-spec spec)
    (update flock :logs conj (str "No filespec for file " (:file-name flock)))))

(defn- pipe-get-file [flock]
  (try (cond (not (:file-spec flock)) (update flock :logs conj (str "No file spec, didn't get anything"))
             (= :embedded (:location flock)) (assoc flock :file {:Body (get-in flock [:event :data])
                                                                 :file-name (get-in flock [:event :file-name])
                                                                 :source :embedded})
             :else (assoc flock :file (get-file! (:file-name flock) (:location flock))))
       (catch java.io.FileNotFoundException e
         (throw (ex-info "File not found" (-> flock
                                              (update :errors  conj [{:error-message "File not found"
                                                                      :data (select-keys flock [:file-name :location])}])
                                              (assoc :job-status :failed)))))))

(defn- pipe-decrypt [flock]
  (if-let [{:keys [key-loc password]} (-> flock :file-spec :decryption)]
    (-> flock
        (update-in [:file :Body] #(decrypt key-loc password %))
        (update :logs conj "Successfully decrypted"))
    (update flock :logs conj "No decryption")))

(defn- pipe-identify [flock]
  (assoc flock :instance-identity
         (apply merge (for [rule (get-in flock [:file-spec :instance-identity])]
                        (establish-identity (get flock :file) rule)))))

(defn- obscure-secrets
  "Removes any sensitive information from the flock before it is emitted"
  [flock]
  (cond-> flock
    (get-in flock [:file-spec :decryption])
    (assoc-in [:file-spec :decryption] true)
    :always (update :logs conj "secrets obscured")))

(defn- persist-job-run! [flock]
  (when (not (get-in flock [:event :dry-run])) (swap! job-runs assoc (:run-id flock) flock))
  flock)

(defn- process-pipe [flock]
  (-> flock (pipe-file-spec)
      (pipe-get-file)
      (pipe-decrypt)
      (pipe-identify)
      (obscure-secrets)
      (assoc :process-end (now))
      (assoc :job-status :succeeded)))

(defn process-file-event! [event]
  (persist-job-run! (try (-> (pipe-prep event)
                             (process-pipe))
                         (catch clojure.lang.ExceptionInfo e (ex-data e)))))

(comment

  (process-file-event! {:file-name "test.txt" :location :local})
  (process-file-event! {:file-name "notinterested.txt" :location :local})
  (process-file-event! {:file-name "2021-05-21_trades.csv" :location :local})
  (process-file-event! {:file-name "portfolios.csv" :location :local
                        :run-id (java.util.UUID/randomUUID)})
  (process-file-event! {:file-name "encrypt.txt.pgp"
                        :location :local
                        :run-id (java.util.UUID/randomUUID)})
  (process-file-event! {:file-name "doesntexist.txt"
                        :location :local
                        :run-id (java.util.UUID/randomUUID)})

  (process-file-event! {:file-name "test.txt" :location :embedded
                        :data "hello world"
                        :run-id (uuid)})
  (process-file-event! {:file-name "test.txt" :location :embedded
                        :data "hello world"
                        :run-id (uuid)
                        :dry-run true})

  @job-runs
  (reset! job-runs {})

  (process-file-event! {:file-name "encrypt.txt.pgp" :location :s3})

  (map #(process-file-event! {:file-name % :location :local})
       ["encrypt.txt.pgp" "test.txt" "notinterested.txt" "2021-05-21_trades.csv"])

  1)
