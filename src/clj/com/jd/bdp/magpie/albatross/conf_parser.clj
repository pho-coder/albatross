(ns com.jd.bdp.magpie.albatross.conf-parser
  (:require [clojure.tools.logging :as log])
  (:import (com.jd.extservice.impl PlumberServiceImpl)
           (com.jd.exception BuffaloException)
           (java.util Date)))

(def PREFIX "*p*")
(def BASE-CONF {:jar "magpie-test-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_test_plumber_task.core"
                :group "default"
                :type "memory"})

(defn- generate-task-name
  "plumber*p*albatross-test-0*p*test-job*p*task-1"
  [albatross-id job-id index]
  (str "plumber" PREFIX albatross-id PREFIX job-id PREFIX "task-" index))

(defn template-bean->map
  ; 因为bean方法不能迭代地执行，把JavaBean内部的JavaBean变成 Clojure Map
  ; 没有找到合适的方法，只好用原始手段构造
  [abean]
  (let [amap (bean abean)
        ext-target-ds (bean (:extTargetDs amap))
        ext-src-ds-list (map bean (:extSrcDsList amap))]
    (assoc amap :extTargetDs ext-target-ds :extSrcDsList (take 1 ext-src-ds-list))))

(defn get-template-bean
  [template-id]
  (log/info (Date.))
  (let [ps (PlumberServiceImpl.)
        template-bean (try
                        (.getTemplateById ps template-id)
                        (catch BuffaloException e (.printStackTrace e) (log/error e) nil))]
    (template-bean->map template-bean)))

(defn- generate-task-conf
  [job-id task-list]
  (reduce (fn [[tasks i] item]
            (let [task-id (generate-task-name "albatross-test-0" job-id i)
                  conf (into BASE-CONF item)]
              [(assoc tasks task-id conf) (inc i)]))
          [{} 1]
          task-list))

(defn- default-split-fn
  "默认拆分方法：拆分任务为多个子任务，按数据源分"
  [template-bean job-id]
  (log/info (Date.))
  (let [extSrcDsList (:extSrcDsList template-bean)
        base-bean (assoc template-bean :extSrcDsList [])
        afn (fn [src] (assoc base-bean :extSrcDsList [src]))
        task-list (map afn extSrcDsList)
        [confs _] (generate-task-conf job-id task-list)]
    confs))

(defn parse
  "return struct:
  strategy-fn return a map :
  {task-id [;; one thread in task one map
            {:source source :target target :sql sql :jar jar :klass klass :group group :type type}}"
  [job-id & {:keys [strategy-fn]
             :or {strategy-fn default-split-fn}}]
  (let [task-confs (strategy-fn (get-template-bean job-id) job-id)]
    (clojure.pprint/pprint task-confs))

  ;临时返回的测试值
  {"plumber*p*albatross-test-0*p*test-job*p*task-0" {:source "mysql"
                                                     :target "hadoop"
                                                     :sql "select * from a"
                                                     :jar "magpie-test-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                                                     :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_test_plumber_task.core"
                                                     :group "default"
                                                     :type "memory"}
   "plumber*p*albatross-test-0*p*test-job*p*task-1" {:source "mysql"
                                                     :target "hadoop"
                                                     :sql "select * from b"
                                                     :jar "magpie-test-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                                                     :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_test_plumber_task.core"
                                                     :group "default"
                                                     :type "memory"}})
