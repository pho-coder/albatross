(ns com.jd.bdp.magpie.albatross.thrift.services
  (:require [thrift-clj.core :as thrift]))

(defn coast-heartbeat
  [uuid jobid]
  (str uuid " " jobid))

(thrift/import
 (:services com.jd.bdp.magpie.albatross.generated.Coast))

(thrift/defservice coast-heartbeat-service
  Coast
  (heartbeat [uuid jobid]
             (coast-heartbeat uuid jobid)))
