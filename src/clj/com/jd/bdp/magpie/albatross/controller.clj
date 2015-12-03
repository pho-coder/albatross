(ns com.jd.bdp.magpie.albatross.controller
  (:use [com.jd.bdp.magpie.albatross.bootstrap])
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.util.utils :as magpie-utils])
  (:import [java.util.concurrent LinkedBlockingQueue]))

;; {jobid : {:uuid uuid :start-time start-time :update-time update-time}}
(def ^:dynamic *all-jobs* (atom (hash-map)))
(def ^:dynamic operations-queue (LinkedBlockingQueue. 10000))

(defn check-job-exist?
  [jobid]
  (contains? @*all-jobs* jobid))

(defn get-uuid
  [jobid]
  (get (get @*all-jobs* jobid) :uuid))

(defn add-job
  [uuid jobid]
  (if (check-job-exist? jobid)
    (swap! *all-jobs* assoc-in [jobid :update-time] (magpie-utils/timestamp2datetime (magpie-utils/current-time-millis)))
    (swap! *all-jobs* assoc-in [jobid] {:uuid uuid
                                        :start-time (magpie-utils/timestamp2datetime (magpie-utils/current-time-millis))
                                        :update-time (magpie-utils/timestamp2datetime (magpie-utils/current-time-millis))})))

(defn deal-coast-operations
  []
  (while (> (.size operations-queue) 0)
    (let [operation (.poll operations-queue)]
      (case operation
        OPERATION-TYPE-ADD-JOB (add-job (:uuid operation)
                                        (:jobid operation))
        (log/error "operation type error: " operation)))))
