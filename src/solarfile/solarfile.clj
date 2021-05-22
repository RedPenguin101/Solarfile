(ns solarfile.solarfile
  (:require [cognitect.aws.client.api :as aws]
            [clojure.edn :as edn]
            [clj-pgp.core :as pgp]
            [clj-pgp.keyring :as keyring]
            [clj-pgp.message :as pgp-msg])
  (:gen-class))

(def config (edn/read-string (slurp "resources/.secrets.edn")))
(def s3 (aws/client {:api :s3}))
(aws/validate-requests s3 true)

(defn read-body-stream [file] (update file :Body slurp))

(defn get-and-check [bucket key]
  (let [response (aws/invoke s3 {:op :GetObject :request {:Bucket bucket :Key key}})]
    (if (:Error response)
      (throw (ex-info "Error getting file" response))
      response)))

(defn get-file! [file-key protocol]
  (case protocol
    :s3    (read-body-stream (assoc (get-and-check (-> config :s3 :bucket-name) file-key) :file-name file-key :source :s3))
    :local {:Body (slurp (str "resources/" file-key)) :file-name file-key :source :local}))

(comment
  "objects come back from S3 like this:"
  {:LastModified #inst "2021-05-22T09:57:13.000-00:00"
   :ETag "\"098f6bcd4621d373cade4e832627b4f6\""
   :Metadata {}
   :ContentLength 4
   :ContentType "text/plain"
   :AcceptRanges "bytes"
   :Body "test"
   :filename "test.txt"})

(def file-specs
  {:some-file       {:mask #"trades.csv"
                     :format :csv
                     :instance-identity [{:name :business-date :lookin :filename :pattern #"(\d{4}-\d{2}-\d{2})"}]
                     :expectations []
                     :endpoints []}
   :some-other-file {:mask #"test.txt"
                     :format :txt
                     :instance-identity false
                     :expectations []
                     :endpoints []}
   :encrypted-file  {:mask #"encrypt.txt.pgp"
                     :format :txt
                     :instance-identity false
                     :decryption {:key-loc "resources/keys/privkey.asc"
                                  :password "welcome"}
                     :expectations []
                     :endpoints []}})

(defn find-file-spec [filename specs]
  (let [matches (keep (fn [[spec spec-def]] (when (re-find (:mask spec-def) filename) spec)) specs)]
    (cond
      (= (count matches) 1) ((first matches) specs)
      (> (count matches) 1) {:errors [["Multiple filespec matches" matches]]}
      (zero? (count matches)) nil)))

(comment
  (find-file-spec "anothertest.txt" file-specs)
  (find-file-spec "test.txt" file-specs)
  (find-file-spec "nomatch.txt" file-specs)
  1)

(defn decrypt [key-loc password message]
  (let [keyring (keyring/load-secret-keyring (slurp key-loc))
        pubkey (first (keyring/list-public-keys keyring))
        seckey (keyring/get-secret-key keyring (pgp/hex-id pubkey))
        privkey (pgp/unlock-key seckey password)]
    (pgp-msg/decrypt message privkey)))

(comment
  (decrypt "resources/privkey.asc" "welcome" (slurp "resources/encrypt.txt.pgp")))

(defn pipe-prep [event]
  {:event event
   :file-name (:file-name event)
   :location (:location event)
   :logs []})

(defn pipe-file-spec [flock]
  (if-let [spec (find-file-spec (:file-name flock) file-specs)]
    (assoc flock :file-spec spec)
    (update flock :logs conj (str "No filespec for file " (:file-name flock)))))

(defn pipe-get-file [flock]
  (if (:file-spec flock)
    (assoc flock :file (get-file! (:file-name flock) (:location flock)))
    (update flock :logs conj (str "No file spec, didn't get anything"))))

(defn pipe-decrypt [flock]
  (if-let [d (-> flock :file-spec :decryption)]
    (-> flock
        (update-in [:file :Body] #(decrypt (:key-loc d) (:password d) %))
        (update :logs conj "Successfully decrypted"))
    (update flock :logs conj "No decryption")))

(defn obscure-secrets [flock]
  (cond-> flock
    (get-in flock [:file-spec :decryption]) (assoc-in [:file-spec :decryption] true)))

(defn process-file-event! [event]
  (-> event
      (pipe-prep)
      (pipe-file-spec)
      (pipe-get-file)
      (pipe-decrypt)
      (obscure-secrets)))

(comment
  (process-file-event! {:file-name "encrypt.txt.pgp" :location :local})
  (process-file-event! {:file-name "test.txt" :location :local})
  (process-file-event! {:file-name "notinterested.txt" :location :local})
  (process-file-event! {:file-name "encrypt.txt.pgp" :location :s3})

  1)

