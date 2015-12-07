(ns com.jd.bdp.magpie.albatross.controller
  (:use [com.jd.bdp.magpie.albatross.bootstrap])
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.util.utils :as magpie-utils])
  (:import [java.util.concurrent LinkedBlockingQueue]))

;; {jobid {:uuid uuid :start-time start-time :update-time update-time :status init}}
(def ^:dynamic *all-jobs* (atom (hash-map)))
(def ^:dynamic coast-operations-queue (LinkedBlockingQueue. 10000))

;; {jobid {appid {:start-time start-time :update-time update-time :status init}}}
(def ^:dynamic *all-apps* (atom (hash-map)))
(def ^:dynamic app-heartbeats-queue (LinkedBlockingQueue. 10000))

(defn check-job-in-jobs?
  [jobid]
  (contains? @*all-jobs* jobid))

(defn check-job-in-apps?
  [jobid]
  (contains? @*all-apps* jobid))

(defn get-uuid
  [jobid]
  (get (get @*all-jobs* jobid) :uuid))

(defn get-job-status
  [jobid]
  (get (get @*all-jobs* jobid) :status))

(defn check-yours?
  [uuid jobid]
  (let [job (get @*all-jobs* jobid)
        uuid-now (:uuid job)]
    (if (= uuid uuid-now)
      {:yours? true
       :info job}
      {:yours? false
       :info job})))

(defn add-apps!
  [jobid]
  (if (check-job-in-apps? jobid)
    (log/warn jobid " is in *all-apps*")
    (let [now (magpie-utils/current-time-millis)]
      (swap! *all-apps* assoc-in [jobid] {"test-app-0" {:start-time now
                                                        :update-time now
                                                        :status STATUS-INIT}
                                          "test-app-1" {:start-time now
                                                        :update-time now
                                                        :status STATUS-INIT}}))))

(defn add-job!
  [uuid jobid]
  (if (check-job-in-jobs? jobid)
    (swap! *all-jobs* assoc-in [jobid :update-time] (magpie-utils/current-time-millis))
    (do (swap! *all-jobs* assoc-in [jobid] {:uuid uuid
                                            :start-time (magpie-utils/current-time-millis)
                                            :update-time (magpie-utils/current-time-millis)
                                            :status STATUS-INIT})
        (add-apps! jobid))))

(defn update-job!
  [uuid jobid]
  (let [yours (check-yours? uuid jobid)]
    (if (:yours? yours)
      (swap! *all-jobs* assoc-in [jobid :update-time] (magpie-utils/current-time-millis))
      (log/error "update job error by uuid:" uuid "job:" (:info yours)))))

(defn deal-coast-operations!
  []
  (while (> (.size coast-operations-queue) 0)
    (let [operation (.poll coast-operations-queue)
          type (:type operation)
          uuid (:uuid operation)
          jobid (:jobid operation)]
      (condp = type
        OPERATION-TYPE-ADD-JOB (add-job! uuid
                                         jobid)
        OPERATION-TYPE-UPDATE-JOB (update-job! uuid
                                               jobid)
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

(defn check-app-status!
  []
  nil)

(defn deal-coasts!
  []
  (check-job-status!)
  (deal-coast-operations!))

(defn deal-app-heartbeats!
  []
  (doseq [one @*all-jobs*]
    (let [jobid (key one)
          value (val one)
          uuid (:uuid value)
          start-time (:start-time value)
          update-time (:update-time value)
          status (:status value)
          now (magpie-utils/current-time-millis)]
      (condp = status
        STATUS-INIT (if (<= (.nextInt (java.util.Random.) 10) 1)
                      (swap! *all-jobs* assoc-in [jobid :status] STATUS-RUNNING))
        STATUS-RUNNING (if (<= (.nextInt (java.util.Random.) 10) 1)
                         (swap! *all-jobs* assoc-in [jobid :status] STATUS-FINISH)
                         (swap! *all-jobs* assoc-in [jobid :status] STATUS-STOP))
        STATUS-FINISH nil
        STATUS-STOP nil
        (log/error status "error in deal-app-heartbeats!")))))

(defn deal-apps!
  []
  (check-app-status!)
  (deal-app-heartbeats!))

(defn diff-jobs-apps!
  []
  nil)
