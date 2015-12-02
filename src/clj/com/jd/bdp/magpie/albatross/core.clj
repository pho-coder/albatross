(ns com.jd.bdp.magpie.albatross.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.magpie-framework-clj.task-executor :as task-executor]
            [thrift-clj.core :as thrift]

            [com.jd.bdp.magpie.util.utils :as magpie-utils]
            [com.jd.bdp.magpie.albatross.thrift.server :as server]
            [com.jd.bdp.magpie.albatross.thrift.services :as services]
            [com.jd.bdp.magpie.albatross.util.utils :as utils]
            [com.jd.bdp.magpie.albatross.heartbeats :as heartbeats]))

(defn prepare-fn
  [job-id]
  (let [albatrosses-path "/albatrosses/"
        albatross-node (str albatrosses-path job-id)]
    (while (not (utils/create-albatross-node albatross-node))
      (log/warn "zk albatross node exists:" job-id)
      (Thread/sleep 1000))
    (server/start-server services/coast-heartbeat-service)
    (utils/set-albatross-info albatross-node {:ip (magpie-utils/get-ip)
                                              :port @server/*coast-server-port*
                                              :jobs-num 0
                                              :apps-num 0})
    (log/info "albatross" job-id "is flying!")))

(defn run-fn [job-id]
  (log/info job-id "run!")
  (Thread/sleep 3000)
  (log/info "coast server:" @server/*coast-server*)
  (log/info "coast server port:" @server/*coast-server-port*)
  (log/info "coasts:" @heartbeats/*all-jobs*))

(defn close-fn [job-id]
  (log/info job-id "close!"))

(defn -main
  [& args]
  (try
    (task-executor/execute run-fn :prepare-fn prepare-fn)
    (catch Throwable e
      (log/error e))))
