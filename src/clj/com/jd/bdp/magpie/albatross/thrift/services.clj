(ns com.jd.bdp.magpie.albatross.thrift.services
  (:require [thrift-clj.core :as thrift]

            [com.jd.bdp.magpie.albatross.heartbeats :as heartbeats]
            [com.jd.bdp.magpie.albatross.controller :as controller]))

(thrift/import
 (:services com.jd.bdp.magpie.albatross.generated.Albatross))

(thrift/defservice coast-heartbeat-service
  Albatross
  (coastHeartbeat [uuid jobid]
                  (heartbeats/coast-heartbeat uuid jobid))
  (islandHeartbeat [jobid taskid status]
                   (heartbeats/island-heartbeat jobid taskid status))
  (getTaskConf [jobid taskid]
               (controller/get-task-conf jobid taskid)))
