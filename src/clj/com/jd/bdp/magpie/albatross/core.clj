(ns com.jd.bdp.magpie.albatross.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]
            [thrift-clj.core :as thrift]

            [com.jd.bdp.magpie.albatross.thrift.server :as server]
            [com.jd.bdp.magpie.albatross.thrift.services :as services]))

(defn prepare-fn [job-id]
  (log/info job-id "prepare!")
  (server/start-server services/coast-heartbeat-service))

(defn run-fn [job-id]
  (log/info job-id "run!")
  (Thread/sleep 3000)
  (log/info "coast server port:" @server/*coast-server-port*))

(defn close-fn [job-id]
  (log/info job-id "close!"))

(defn -main
  [& args]
  (log/info "albatross is flying!")
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e))))
