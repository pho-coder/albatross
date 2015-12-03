(ns com.jd.bdp.magpie.albatross.thrift.server
  (:require [thrift-clj.core :as thrift]
            [clojure.tools.logging :as log]
;            [taoensso.timbre :as log]
)
  (:import [org.apache.thrift.transport TTransportException]))

(def ^:dynamic *coast-server* (atom nil))
(def ^:dynamic *ports* (atom nil))
(def ^:dynamic *coast-server-port* (atom nil))

(defn get-random-port
  []
  (if (nil? @*ports*)
    (do (reset! *ports* (set (range 37000 38000)))
        (reset! *coast-server-port* 38000)
        38000)
    (if (empty? @*ports*)
      (throw (RuntimeException. "local port filled!"))
      (let [port (.next (.iterator @*ports*))]
        (reset! *ports* (disj @*ports* port))
        (reset! *coast-server-port* port)
        port))))

(defn get-server
  [service]
  (loop [server nil]
    (if (nil? server)
      (recur (try
               (thrift/serve!
                (thrift/multi-threaded-server
                 service (get-random-port)
                 :protocol :compact))
               (catch TTransportException e
                 (log/warn e)
                 nil)))
      server)))

(defn start-server
  [service]
  (reset! *coast-server* (get-server service)))
