(ns com.jd.bdp.magpie.albatross.controller
  (:use [com.jd.bdp.magpie.albatross.bootstrap])
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.util.utils :as magpie-utils])
  (:import [java.util.concurrent LinkedBlockingQueue]))

;; {jobid : {:uuid uuid :start-time start-time :update-time update-time :status init}}
(def ^:dynamic *all-jobs* (atom (hash-map)))
(def ^:dynamic operations-queue (LinkedBlockingQueue. 10000))

(defn check-job-exist?
  [jobid]
  (contains? @*all-jobs* jobid))

(defn get-uuid
  [jobid]
  (get (get @*all-jobs* jobid) :uuid))

(defn check-yours?
  [uuid jobid]
  (let [job (get @*all-jobs* jobid)
        uuid-now (:uuid job)]
    (if (= uuid uuid-now)
      {:yours? true
       :info job}
      {:yours? false
       :info job})))

(defn add-job!
  [uuid jobid]
  (if (check-job-exist? jobid)
    (swap! *all-jobs* assoc-in [jobid :update-time] (magpie-utils/current-time-millis))
    (swap! *all-jobs* assoc-in [jobid] {:uuid uuid
                                        :start-time (magpie-utils/current-time-millis)
                                        :update-time (magpie-utils/current-time-millis)})))

(defn update-job!
  [uuid jobid]
  (let [yours (check-yours? uuid jobid)]
  (if (:yours? yours)
    (swap! *all-jobs* update-in [jobid :update-time] (magpie-utils/current-time-millis))
    (log/error "update job error by uuid:" uuid "job:" (:info yours)))))

(defn deal-coast-operations!
  []
  (while (> (.size operations-queue) 0)
    (let [operation (.poll operations-queue)]
      (case (:type operation)
        OPERATION-TYPE-ADD-JOB (add-job! (:uuid operation)
                                         (:jobid operation))
        OPERATION-TYPE-UPDATE-JOB (update-job! (:uuid operation)
                                               (:jobid operation))
        (log/error "operation type error: " operation)))))

(defn check-job-status!
  []
  (doseq [one @*all-jobs*]
    (let [jobid (key one)
          value (val one)
          uuid (:uuid value)
          start-time (:start-time value)
          update-time (:update-time value)
          status (:status value)
          now (magpie-utils/current-time-millis)]
      (if (> (- now update-time) DEAD-TIMEOUT-MILLIS)
        (do (swap! *all-jobs* dissoc jobid)
            (log/info "delete dead job:" one))))))
