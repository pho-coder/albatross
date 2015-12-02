(ns com.jd.bdp.magpie.albatross.heartbeats
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(def HEARTBEAT-COAST "heartbeat-coast")
(def STATUS-INIT "init")
(def STATUS-RUNNING "running")
(def STATUS-REJECT "reject")
(def OPERATION-TYPE-ADD-JOB "add-job")

;; {jobid : {:uuid uuid :start-time start-time}}
(def ^:dynamic *all-jobs* (atom (hash-map)))
(def ^:dynamic operations-queue (LinkedBlockingQueue. 10000))

(defn check-job-exist?
  [jobid]
  (contains? @*all-jobs* jobid))

(defn get-uuid
  [jobid]
  (get (get @*all-jobs* jobid) :uuid))

(defn add-new-job
  [uuid jobid]
  (.put operations-queue {:type OPERATION-TYPE-ADD-JOB
                          :uuid uuid
                          :jobid jobid})
  (loop [retry-times 5]
    (if (<= retry-times 0)
      {:status STATUS-INIT}
      (let [existing-uuid (get-uuid jobid)]
      (if-not (nil? existing-uuid)
        (if (= uuid existing-uuid)
          {:status STATUS-RUNNING}
          {:status STATUS-REJECT
           :info (str jobid " has been submmited by " existing-uuid " NOT YOU!")})
        (do (Thread/sleep 300)
            (recur (- retry-times 1))))))))

(defn coast-heartbeat
  [uuid jobid]
  (if (check-job-exist? jobid)
    (let [existing-uuid (get-uuid jobid)]
      (if (= uuid existing-uuid)
        (json/write-str {:status STATUS-RUNNING})
        (json/write-str {:status STATUS-REJECT
                         :info (str jobid " has been submmited by " existing-uuid " NOT YOU!")})))
    (json/write-str (add-new-job uuid jobid))))
