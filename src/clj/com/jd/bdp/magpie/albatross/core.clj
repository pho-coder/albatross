(ns com.jd.bdp.magpie.albatross.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]))

(defn prepare-fn [job-id]
  (log/info job-id "prepare!"))

(defn run-fn [job-id]
  (log/info job-id "run!")
  (Thread/sleep 3000))

(defn close-fn [job-id]
  (log/info job-id "close!"))

(defn -main
  [& args]
  (log/info "albatross is flying!")
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e))))
