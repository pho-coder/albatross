(ns com.jd.bdp.magpie.albatross.thrift.services)

(defn coast-heartbeat
  [uuid jobid]
  (str uuid " " jobid))
