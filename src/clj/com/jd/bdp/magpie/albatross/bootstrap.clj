(ns com.jd.bdp.magpie.albatross.bootstrap)

(use 'clojure.set)

(def HEARTBEAT-COAST "heartbeat-coast")
(def THREAD-NUM 10)
(def STATUS-INIT "init")
(def STATUS-RUNNING "running")
(def STATUS-REJECT "reject")
(def STATUS-FINISH "finish")
(def STATUS-STOP "stop")
(def OPERATION-TYPE-ADD-JOB "add-job")
(def OPERATION-TYPE-UPDATE-JOB "update-job")
(def DEAD-TIMEOUT-MILLIS 20000)
(def ALBATROSS-SERVICE-NAME "albatross-service")
(def SEPARATOR "*p*")

(def DICT-BASE-URL "http://atom.bdp.jd.local/api/wordbook/getValue")
(def DICT-APP-ID "bdp.jd.com")
(def DICT-TOKEN "RQLMPXULF3EG23CPZL3U257B7Y")
(def DICT-USER-ERP "cengguangyao")

(def BASE-CONF-KEY {:jar "com.jd.bdp.magpie.albatross.plumber.mysql2hadoop.jar"
                    :klass "com.jd.bdp.magpie.albatross.plumber.mysql2hadoop.class"
                    :group "com.jd.bdp.magpie.albatross.plumber.mysql2hadoop.group"
                    :type "com.jd.bdp.magpie.albatross.plumber.mysql2hadoop.type"})
