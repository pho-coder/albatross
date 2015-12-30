(ns com.jd.bdp.magpie.albatross.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]
            [thrift-clj.core :as thrift]

            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [com.jd.bdp.magpie.albatross.thrift.server :as server]
            [com.jd.bdp.magpie.albatross.thrift.services :as services]
            [com.jd.bdp.magpie.albatross.util.utils :as utils]
            [com.jd.bdp.magpie.albatross.controller :as controller]
            [com.jd.bdp.magpie.albatross.thrift.client :as client]))

(defn prepare-fn
  [job-id]
  (reset! controller/albatross-id job-id)
  (let [albatross-path "/albatross"
        albatrosses-path (str albatross-path "/albatrosses/")
        albatross-node (str albatrosses-path job-id)]
    (while (not (utils/create-albatross-node albatross-node))
      (log/warn "zk albatross node exists:" job-id)
      (Thread/sleep 1000))
    (server/start-server services/coast-heartbeat-service)
    (utils/set-albatross-info albatross-node {:ip (magpie-utils/get-ip)
                                              :port @server/*coast-server-port*
                                              :jobs-num 0
                                              :tasks-num 0})
    (client/get-nimbus-client)
    (log/info "albatross" job-id "is flying!")))

(defn run-fn [job-id]
  (log/info job-id "run!")
  (Thread/sleep 1000)
  (when @client/*reset-nimbus-client*
    (client/get-nimbus-client)
    (reset! client/*reset-nimbus-client* false))
  (log/info "coast server port:" @server/*coast-server-port*)
  (log/info "coast operations queue size:" (.size controller/coast-operations-queue))
  (log/info "jobs:" @controller/*all-jobs*)
  (log/info "tasks:" @controller/*all-tasks*)
  (controller/deal-tasks!)
  (controller/deal-coasts!))

(defn close-fn [job-id]
  (log/info job-id "close!"))

(defn -main
  [& args]
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e))))
