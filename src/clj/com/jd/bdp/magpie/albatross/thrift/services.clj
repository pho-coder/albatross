(ns com.jd.bdp.magpie.albatross.thrift.services
  (:require [thrift-clj.core :as thrift]

            [com.jd.bdp.magpie.albatross.heartbeats :as heartbeats]))

(thrift/import
 (:services com.jd.bdp.magpie.albatross.generated.Coast))

(thrift/defservice coast-heartbeat-service
  Coast
  (heartbeat [uuid jobid]
              (heartbeats/coast-heartbeat uuid jobid)))
