(ns com.jd.bdp.magpie.albatross.conf-parser
  (:require [clojure.tools.logging :as log])
  (:import (com.jd.extservice.impl PlumberServiceImpl)
           (com.jd.exception BuffaloException)
           (java.util Date)))

(def SEPARATOR "*p*")
; TODO 以下信息来自哪里？
(def BASE-CONF {:jar "magpie-test-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_test_plumber_task.core"
                :group "default"
                :type "memory"})

(defn- generate-task-name
  "plumber*p*albatross-test-0*p*test-job*p*task-1"
  [albatross-id job-id index]
  (str "plumber" SEPARATOR albatross-id SEPARATOR job-id SEPARATOR "task-" index))

(defn template-bean->map
  " 因为bean方法不能迭代地执行，把JavaBean内部的JavaBean变成Map
    没有找到合适的方法之前，只好用原始手段构造"
  [abean]
  (let [amap (bean abean)
        ext-target-ds (bean (:extTargetDs amap))
        ext-src-ds-list (map bean (:extSrcDsList amap))]
    (assoc amap :extTargetDs ext-target-ds :extSrcDsList ext-src-ds-list)))

(defn get-template-bean
  [template-id]
  (log/info (Date.))
  (let [ps (PlumberServiceImpl.)
        template-bean (try
                        (.getTemplateById ps template-id)
                        (catch BuffaloException e (.printStackTrace e) (log/error e) nil))]
    (template-bean->map template-bean)))

(defn- generate-task-conf
  "return a vector: [task-confs task-num]"
  [job-id task-list]
  (reduce (fn [[tasks i] item]
            (let [task-id (generate-task-name "albatross-test-0" job-id i)
                  conf (into BASE-CONF item)]
              [(assoc tasks task-id conf) (inc i)]))
          [{} 1]
          task-list))

(defn- default-split-fn
  [template-bean job-id]
  (log/info (Date.))
  (let [extSrcDsList (:extSrcDsList template-bean)
        afn (fn [src] (assoc template-bean :extSrcDsList [src]))  ; 保持conf结构与原来一致，但extSrcList只有一个元素
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
  ; (strategy-fn (get-template-bean job-id) job-id)
  ; TODO 以下在测试时使用
  (let [conf-list (strategy-fn (get-template-bean job-id) job-id)]
    (map (fn [[task-id conf]]
           (let [ext-tar-ds (:extTargetDs conf)
                 target-table (:targetTableName conf)
                 sql (:sql conf)
                 target-path (str (:extend1 ext-tar-ds) "/" target-table)
                 ext-src-ds (first (:extSrcDsList conf))
                 host (:dbHost ext-src-ds)
                 user (:dbUser ext-src-ds)
                 password (:dbPassword ext-src-ds)
                 src-db-name (:dbName ext-src-ds)
                 source (:dbType ext-src-ds)]
             ; 最终的配置
             {task-id {:source source
                       :target target-path
                       :host host
                       :sqls [sql]
                       :db-name src-db-name
                       :user user
                       :password password
                       :jar "magpie-mysql2hadoop-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                       :klass "com.jd.bdp.magpie.magpie-eggs-clj.magpie-mysql2hadoop-plumber-task.core"
                       :group "default"
                       :type "memory"}}))
         conf-list)))
