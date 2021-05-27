(ns solarfile.solarfile-test
  (:require [clojure.test :refer [deftest is testing]]
            [solarfile.solarfile :as SUT]))

(defn failed? [job-run] (= (:job-status job-run) :failed))
(defn success? [job-run] (= (:job-status job-run) :succeeded))
(defn ignored? [job-run] (= (:job-status job-run) :ignored))

(deftest events-test
  ;; impure, uses file-specs defined in solarfile. Do better
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

(deftest specs-test
  (let [test-specs {:test {:mask #"test.txt"}}
        test-job {:event {}
                  :run-id :test-run
                  :location :embedded
                  :file-name "test.txt"}]
    (is (success? (SUT/process-pipe test-job test-specs)))
    (is (ignored? (SUT/process-pipe (assoc test-job :file-name "test-no-spec.txt") test-specs)))))

