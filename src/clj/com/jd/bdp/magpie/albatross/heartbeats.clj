(ns com.jd.bdp.magpie.albatross.heartbeats
  (:use [com.jd.bdp.magpie.albatross.bootstrap])
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            
            [com.jd.bdp.magpie.albatross.controller :as controller]))

(defn add-new-job
  [uuid jobid]
  (.put controller/coast-operations-queue {:type OPERATION-TYPE-ADD-JOB
                                           :uuid uuid
                                           :jobid jobid}))

(defn update-job-heartbeat
  [uuid jobid]
  (.put controller/coast-operations-queue {:type OPERATION-TYPE-UPDATE-JOB
                                           :uuid uuid
                                           :jobid jobid}))

(defn coast-heartbeat
  [uuid jobid]
  (if (controller/check-job-in-jobs? jobid)
    (let [existing-uuid (controller/get-uuid jobid)]
      (if (= uuid existing-uuid)
        (do (update-job-heartbeat uuid jobid)
            (json/write-str {:status (controller/get-job-status jobid)}))
        (json/write-str {:status STATUS-REJECT
                         :info (str jobid " has been submmited by " existing-uuid " NOT YOU!")})))
    (json/write-str (add-new-job uuid jobid))))

(defn update-task-heartbeat
  [jobid taskid status]
  (.put controller/task-heartbeats-queue {:job-id jobid
                                         :task-id taskid
                                         :status status}))

(defn island-heartbeat
  [jobid taskid status]
  (update-task-heartbeat jobid taskid status))
