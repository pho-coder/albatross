(ns com.jd.bdp.magpie.albatross.heartbeat)

(defn coast-heartbeat
  [uuid jobid]
  (str uuid " " jobid))
