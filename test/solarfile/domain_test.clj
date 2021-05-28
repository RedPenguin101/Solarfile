(ns solarfile.domain-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clj-pgp.keyring :as keyring]
            [solarfile.domain :as SUT]))

(def keyring (keyring/load-secret-keyring (slurp (io/resource "keys/privkey.asc"))))

(defn failed? [job-run] (= (:job-status job-run) :failed))
(defn success? [job-run] (= (:job-status job-run) :succeeded))
(defn ignored? [job-run] (= (:job-status job-run) :ignored))

(defn get-file-stub [file-key _] {:Body "Stub" :file-name file-key :source :stub})
(def dummy-run-state (atom {}))

(defn process-event-wrap [event specs]
  (SUT/process-event event specs get-file-stub dummy-run-state keyring))

(deftest events-test
  ;; impure, uses file-specs defined in solarfile. Do better
  (testing "events without file-name, location and run-id result in failed
            job run"
    (let [test-specs {:test {:mask #"test.txt"}}]
      (is (failed? (process-event-wrap {:dry-run true}  test-specs)))
      (is (failed? (process-event-wrap {:file-name "test.txt" :dry-run true} test-specs)))
      (is (failed? (process-event-wrap {:file-name "test.txt" :location :local :dry-run true} test-specs)))
      (is (success? (process-event-wrap {:file-name "test.txt" :location :embedded :data "Hello World"
                                         :run-id :test
                                         :dry-run true}
                                        test-specs))))))

(deftest specs-test
  (let [test-specs {:test {:mask #"test.txt"}}
        test-event {:run-id :test-run
                    :location :embedded
                    :file-name "test.txt"}]
    (is (success? (process-event-wrap test-event test-specs)))
    (is (ignored? (process-event-wrap (assoc test-event
                                             :file-name "test-no-spec.txt")
                                      test-specs)))))
