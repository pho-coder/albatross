(ns com.jd.bdp.magpie.albatross.bootstrap)

(use 'clojure.set)

(def HEARTBEAT-COAST "heartbeat-coast")
(def STATUS-INIT "init")
(def STATUS-RUNNING "running")
(def STATUS-REJECT "reject")
(def STATUS-FINISH "finish")
(def STATUS-STOP "stop")
(def OPERATION-TYPE-ADD-JOB "add-job")
(def OPERATION-TYPE-UPDATE-JOB "update-job")
(def DEAD-TIMEOUT-MILLIS 20000)
(def ALBATROSS-SERVICE-NAME "albatross-service")
