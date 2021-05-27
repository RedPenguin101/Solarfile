(ns solarfile.solarfile-test
  (:require [clojure.test :refer [deftest is testing]]
            [solarfile.solarfile :as SUT]))

(defn failed? [job-run] (= (:job-status job-run) :failed))
(defn success? [job-run] (= (:job-status job-run) :succeeded))

(deftest events-test
  (testing "events without file-name, location and run-id result in failed
            job run"
    (is (failed? (SUT/process-file-event! {:dry-run true})))
    (is (failed? (SUT/process-file-event! {:file-name "test.txt"
                                           :dry-run true})))
    (is (failed? (SUT/process-file-event! {:file-name "test.txt"
                                           :location :local
                                           :dry-run true})))
    (is (success? (SUT/process-file-event! {:file-name "test.txt"
                                            :location :embedded
                                            :data "Hello World"
                                            :run-id #uuid "7a9cc2b5-2771-4bfc-acd2-9965e9b8479f"
                                            :dry-run true})))))
