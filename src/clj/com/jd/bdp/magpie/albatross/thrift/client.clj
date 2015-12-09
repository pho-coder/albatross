(ns com.jd.bdp.magpie.albatross.thrift.client
  (:require [clojure.tools.logging :as log]
            [thrift-clj.core :as thrift]
            
            [com.jd.bdp.magpie.albatross.util.utils :as utils])
  (:import [org.apache.thrift.transport TTransportException]))

(def ^:dynamic *nimbus-client* (atom nil))
(def ^:dynamic *reset-nimbus-client* (atom false))

(thrift/import
 (:clients com.jd.magpie.generated.Nimbus))

(defn get-nimbus-client
  []
  (log/info "get-nimbus-client!")
  (let [nimbus-path "/nimbus"
        nimbus (utils/get-nimbus nimbus-path)]
    (if (nil? nimbus)
      (log/warn "NO nimbus!")
      (let [ip (get nimbus "ip")
            port (get nimbus "port")]
        (try
          (reset! *nimbus-client* (thrift/connect! Nimbus (thrift/framed [ip port]) :protocol :binary))
          (catch Throwable e
            (log/error "get-nimbus-client" e)
            (reset! *reset-nimbus-client* true)))))))

(defn submit-task
  [id jar klass group type]
  (try
    (let [rs (Nimbus/submitTask @*nimbus-client* id jar klass group type)]
      (log/info rs))
    (catch TTransportException e
      (log/error e)
      (reset! *reset-nimbus-client* true))))

(defn operate-task
  [id command]
  (let [rs (Nimbus/operateTask @*nimbus-client* id command)]
    (log/info rs)))
