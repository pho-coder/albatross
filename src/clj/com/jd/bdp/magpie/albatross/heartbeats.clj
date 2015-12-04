(ns com.jd.bdp.magpie.albatross.heartbeats
  (:use [com.jd.bdp.magpie.albatross.bootstrap])
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]

            [com.jd.bdp.magpie.albatross.controller :as controller]))

(defn add-new-job
  [uuid jobid]
  (.put controller/operations-queue {:type OPERATION-TYPE-ADD-JOB
                                     :uuid uuid
                                     :jobid jobid})
  (loop [retry-times 5]
    (if (<= retry-times 0)
      {:status STATUS-INIT}
      (let [existing-uuid (controller/get-uuid jobid)]
      (if-not (nil? existing-uuid)
        (if (= uuid existing-uuid)
          {:status STATUS-RUNNING}
          {:status STATUS-REJECT
           :info (str jobid " has been submmited by " existing-uuid " NOT YOU!")})
        (do (Thread/sleep 300)
            (recur (- retry-times 1))))))))

(defn update-job-heartbeat
  [uuid jobid]
  (.put controller/operations-queue {:type OPERATION-TYPE-UPDATE-JOB
                                     :uuid uuid
                                     :jobid jobid}))

(defn coast-heartbeat
  [uuid jobid]
  (if (controller/check-job-exist? jobid)
    (let [existing-uuid (controller/get-uuid jobid)]
      (if (= uuid existing-uuid)
        (do (update-job-heartbeat uuid jobid)
            (json/write-str {:status (get @controller/*all-jobs* jobid)}))
        (json/write-str {:status STATUS-REJECT
                         :info (str jobid " has been submmited by " existing-uuid " NOT YOU!")})))
    (json/write-str (add-new-job uuid jobid))))
