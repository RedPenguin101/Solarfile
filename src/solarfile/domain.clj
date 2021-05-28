(ns solarfile.domain
  (:require [clojure.data.csv :as csv]
            [clj-pgp.core :as pgp]
            [clj-pgp.keyring :as keyring]
            [clj-pgp.message :as pgp-msg]))

;; File Specs

(defn- find-file-spec [filename specs]
  (let [spec-matches (keep (fn [[spec spec-def]] (when (re-find (:mask spec-def) filename) spec)) specs)]
    (cond
      (= (count spec-matches) 1) (assoc ((first spec-matches) specs) :spec-name (first spec-matches))
      (> (count spec-matches) 1) {:errors [["Multiple filespec matches" spec-matches]]}
      (zero? (count spec-matches)) nil)))

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

;; Decryption

(defn- decrypt [keyring password message]
  (pgp-msg/decrypt message (pgp/unlock-key (keyring/get-secret-key keyring (first (keyring/list-public-keys keyring))) password)))

;;;;;;;;;;;;;;;;; Pipeline functions and pipe ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pipelines operate on "flocks" of data - maps, flowing them through the 
;; pipeline and accreting new data to them through each pipe section
;; Pipe functions just wrap some business functionality so it fits the 
;; pipeline pattern: 
;;                      function :: flock -> flock
;; usually by assoc'ing in a new key. 
;; They usually accrete the pipe-logs at the same time
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- event-check [event]
  (if (every? #(contains? event %) [:file-name :location :run-id])
    event
    (throw (ex-info "Event is missing required keys"
                    {:event event
                     :errors [{:error-message "Event is missing required keys"
                               :data event}]
                     :job-status :failed
                     :run-id (java.util.UUID/randomUUID)}))))

(defn- pipe-prep [event]
  (assoc (merge (select-keys (event-check event) [:file-name :location :run-id])
                {:event event :logs []})
         :job-status :in-progress))

(defn- pipe-file-spec [flock specs]
  (if-let [spec (find-file-spec (:file-name flock) specs)]
    (assoc flock :file-spec spec)
    (-> flock
        (update  :logs conj (str "No filespec for file " (:file-name flock)))
        (assoc :job-status :ignored))))

(defn- pipe-get-file [flock file-fn]
  (try (cond (not (:file-spec flock)) (update flock :logs conj (str "No file spec, didn't get anything"))
             (= :embedded (:location flock)) (assoc flock :file {:Body (get-in flock [:event :data])
                                                                 :file-name (get-in flock [:event :file-name])
                                                                 :source :embedded})
             :else (assoc flock :file (file-fn (:file-name flock) (:location flock))))
       (catch java.io.FileNotFoundException e
         (throw (ex-info "File not found" (-> flock
                                              (update :errors  conj [{:error-message "File not found"
                                                                      :data (select-keys flock [:file-name :location])}])
                                              (assoc :job-status :failed)))))))

(defn- pipe-decrypt [flock keyring]
  (if-let [{:keys [password]} (-> flock :file-spec :decryption)]
    (-> flock
        (update-in [:file :Body] #(decrypt keyring password %))
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

(defn- persist-job-run! [flock job-runs]
  (when (not (get-in flock [:event :dry-run])) (swap! job-runs assoc (:run-id flock) flock))
  flock)

(defn- succeed-if-in-progress [status]
  (if (not (#{:failed :ignored} status)) :succeeded status))

(defn- process-pipe [flock specs file-fn keyring]
  (-> flock (pipe-file-spec specs)
      (pipe-get-file file-fn)
      (pipe-decrypt keyring)
      (pipe-identify)
      (obscure-secrets)
      (update :job-status succeed-if-in-progress)))

(defn process-event [event specs file-fn job-runs keyring]
  (persist-job-run!
   (try (-> (pipe-prep event)
            (process-pipe specs file-fn keyring))
        (catch clojure.lang.ExceptionInfo e (ex-data e)))
   job-runs))
