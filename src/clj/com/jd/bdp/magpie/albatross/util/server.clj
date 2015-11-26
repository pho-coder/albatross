(ns com.jd.bdp.magpie.albatross.util.server
  (:require [thrift-clj.core :as thrift]

            [com.jd.bdp.magpie.albatross.heartbeat :as heartbeat])
;  (:import [com.jd.bdp.magpie.albatross.generated Coast])
  )

(def ^:dynamic *server* (atom nil))
(def ^:dynamic *ports* (atom nil))

(defn get-random-port
  []
  (if (nil? @*ports*)
    (do (reset! *ports* (set (range 37000 38000)))
        38000)
    (if (empty? @*ports*)
      (throw (RuntimeException. "local port filled!"))
      (let [port (.next (.iterator @*ports*))]
        (reset! *ports* (disj @*ports* port))
        port))))

(defn start-server
  []
  (thrift/import
   (:services com.jd.bdp.magpie.albatross.generated.Coast))
  (thrift/defservice coast-heartbeat-service
    com.jd.bdp.magpie.albatross.generated.Coast
    (heartbeat [uuid jobid]
               (heartbeat/coast-heartbeat uuid jobid)))
  (reset! *server*
          (thrift/serve!
           (thrift/multi-threaded-server
            coast-service (get-random-port)
            :bind "127.0.0.1"
            :protocol :compact))))
