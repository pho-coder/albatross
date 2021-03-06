(ns com.jd.bdp.magpie.albatross.controller
  (:use [com.jd.bdp.magpie.albatross.bootstrap])
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [com.jd.bdp.magpie.util.utils :as magpie-utils]

            [com.jd.bdp.magpie.albatross.conf-parser :as parser]
            [com.jd.bdp.magpie.albatross.thrift.client :as client]
            [com.jd.bdp.magpie.albatross.util.utils :as utils])
  (:import [java.util.concurrent LinkedBlockingQueue]))

;; albatross-id
(def ^:dynamic albatross-id (atom nil))

;; {job-id {:uuid uuid :start-time start-time :update-time update-time :status init}}
(def ^:dynamic *all-jobs* (atom (hash-map)))
(def ^:dynamic coast-operations-queue (LinkedBlockingQueue. 10000))

;; {job-id {task-id {:start-time start-time :update-time update-time :status init :conf conf}}}
(def ^:dynamic *all-tasks* (atom (hash-map)))
(def ^:dynamic task-heartbeats-queue (LinkedBlockingQueue. 10000))

(defn check-job-in-jobs?
  [jobid]
  (contains? @*all-jobs* jobid))

(defn check-job-in-tasks?
  [jobid]
  (contains? @*all-tasks* jobid))

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

(defn add-task
  [task-id jar klass group type]
  (if (utils/check-magpie-task-exists? task-id)
    (log/error "task" task-id "EXISTS in /assignments/!")
    (client/submit-task task-id jar klass group type)))

(defn add-tasks!
  [job-id]
  (if (check-job-in-tasks? job-id)
    (log/warn job-id " is in *all-tasks*")
    (let [jobs-path "/albatross/jobs/"
          job-node (str jobs-path job-id)
          now (magpie-utils/current-time-millis)]
      (utils/register-job job-node {"albatross" @albatross-id
                                    "start-time" now
                                    "update-time" now})
      (doseq [task-conf (parser/parse job-id)]
        (let [task-id (key task-conf)
              conf (val task-conf)
              now (magpie-utils/current-time-millis)]
          (swap! *all-tasks* assoc-in [job-id task-id] {:start-time now
                                                        :update-time now
                                                        :status STATUS-INIT
                                                        :conf conf})
          (add-task task-id (:jar conf) (:klass conf) (:group conf) (:type conf)))))))

(defn add-job!
  [uuid job-id]
  (if (check-job-in-jobs? job-id)
    (swap! *all-jobs* assoc-in [job-id :update-time] (magpie-utils/current-time-millis))
    (do (swap! *all-jobs* assoc-in [job-id] {:uuid uuid
                                             :start-time (magpie-utils/current-time-millis)
                                             :update-time (magpie-utils/current-time-millis)
                                             :status STATUS-INIT})
        (add-tasks! job-id))))

(defn update-job!
  [uuid job-id]
  (let [yours (check-yours? uuid job-id)]
    (if (:yours? yours)
      (swap! *all-jobs* assoc-in [job-id :update-time] (magpie-utils/current-time-millis))
      (log/error "update job error by uuid:" uuid "job:" (:info yours)))))

(defn update-job-status!
  [job-id status]
  (swap! *all-jobs* assoc-in [job-id :status] status))

(defn deal-coast-operations!
  []
  (while (pos? (.size coast-operations-queue))
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

(defn delete-jobs!
  [job-id]
  (swap! *all-jobs* dissoc job-id)
  (utils/delete-job-node (str "/albatross/jobs/" job-id)))

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
      (when (> (- now update-time) DEAD-TIMEOUT-MILLIS)
        (delete-jobs! jobid)
        (log/info "delete dead job:" one)))))

(defn delete-tasks!
  [job-id]
  (doseq [one (get @*all-tasks* job-id)]
    (let [task-id (key one)
          task-info (val one)]
      (client/operate-task task-id "kill")))
  (swap! *all-tasks* dissoc job-id))

(defn check-all-tasks-stop?
  [tasks]
  (reduce (fn [a o]
            (or a (if (= (:status (val o)) STATUS-STOP)
                    true
                    false))) false tasks))

(defn check-all-tasks-running?
  [tasks]
  (reduce (fn [a o]
            (and a (if (or (= (:status (val o)) STATUS-RUNNING) (= (:status (val o)) STATUS-FINISH))
                     true
                     false))) true tasks))

(defn check-all-tasks-finish?
  [tasks]
  (reduce (fn [a o]
            (and a (if (= (:status (val o)) STATUS-FINISH)
                     true
                     false))) true tasks))

(defn check-task-status!
  []
  (doseq [one @*all-tasks*]
    (let [job-id (key one)
          tasks (val one)]
      (if-not (contains? @*all-jobs* job-id)
        (do (log/warn "the job:" job-id "NOT exists now!" "DELETE the tasks:" tasks)
            (delete-tasks! job-id))
        (if (check-all-tasks-stop? tasks)
          (do (log/warn "some tasks stopped:" tasks)
              (update-job-status! job-id STATUS-STOP)
              (delete-tasks! job-id))
          (if (check-all-tasks-finish? tasks)
            (do (update-job-status! job-id STATUS-FINISH)
                (delete-tasks! job-id))
            (if (check-all-tasks-running? tasks)
              (update-job-status! job-id STATUS-RUNNING))))))))

(defn deal-coasts!
  []
  (deal-coast-operations!)
  (check-job-status!))

(defn deal-task-heartbeats!
  []
  (while (pos? (.size task-heartbeats-queue))
    (let [one (.poll task-heartbeats-queue)
          job-id (:job-id one)
          task-id (:task-id one)
          status (:status one)
          now (magpie-utils/current-time-millis)]
      (if (nil? (get (get @*all-tasks* job-id) task-id))
        (log/warn job-id task-id "NOT exists in *all-tasks")
      (condp = status
        STATUS-INIT (do (swap! *all-tasks* assoc-in [job-id task-id :update-time] now)
                        (swap! *all-tasks* assoc-in [job-id task-id :status] STATUS-INIT))
        STATUS-RUNNING (do (swap! *all-tasks* assoc-in [job-id task-id :update-time] now)
                           (swap! *all-tasks* assoc-in [job-id task-id :status] STATUS-RUNNING))
        STATUS-FINISH (do (swap! *all-tasks* assoc-in [job-id task-id :update-time] now)
                          (swap! *all-tasks* assoc-in [job-id task-id :status] STATUS-FINISH))
        STATUS-STOP (do (swap! *all-tasks* assoc-in [job-id task-id :update-time] now)
                        (swap! *all-tasks* assoc-in [job-id task-id :status] STATUS-STOP))
        (log/error status "error in deal-task-heartbeats!"))))))

(defn deal-tasks!
  []
  (deal-task-heartbeats!)
  (check-task-status!))

(defn get-task-conf
  [job-id task-id]
  (json/write-str {:job-id job-id
                   :task-id task-id
                   :conf (:conf (get (get @*all-tasks* job-id) task-id))}))
