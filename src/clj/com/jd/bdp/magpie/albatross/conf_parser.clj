(ns com.jd.bdp.magpie.albatross.conf-parser
  (:require [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.albatross.util.template-util :as template]
            [com.jd.bdp.magpie.albatross.util.util :as util])
  (:use com.jd.bdp.magpie.albatross.bootstrap)
  (:import (com.jd.extservice.impl PlumberServiceImpl)
           (com.jd.exception BuffaloException)
           (com.jd.extservice PlumberService)
           (com.jd.model ExtDataSourceBean)
           (com.jd.model.plumber ExtPlumberTemplateBean)))

(def SEPARATOR "*p*")
; TODO 以下信息来自哪里？
(def BASE-CONF {:jar "magpie-mysql2hadoop-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_mysql2hadoop_plumber_task.core"
                :group "default"
                :type "memory"})

(defn- generate-task-name
  [albatross-id job-id uuid]
  (str "plumber" SEPARATOR albatross-id SEPARATOR job-id SEPARATOR uuid))

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
  (let [ps (PlumberServiceImpl.)]
    (try
      (.getTemplateById ps template-id)
      (catch BuffaloException e (.printStackTrace e) (log/error e) nil))))

(defn- get-sub-job-configs
  [template-bean job-id]
  (let [dt "2016-01-07"
        sub-jobs (atom [])
        resource-dept (let [rd (.getResourceDepartment template-bean)]
                        (if-not (or (= "0" rd) (= "1" rd) (= "2" rd))
                          (do (log/error (str "unknown resource department : " rd)) (System/exit 4)) rd))
        original-delete-sql (if (= 0 (.getDeleteStrEnable template-bean))
                              ""
                              (.getDeleteStr template-bean))
        delete-sql (clojure.string/split original-delete-sql #";\n")
        sqlStr (.getSqlStr template-bean)
        where-str (.getWhereStr template-bean)
        where-enable (.getWhereStrEnable template-bean)
        sql (if (= 0 where-enable)
              sqlStr
              (str sqlStr " " where-str))
        data-source (into [] (.getExtSrcDsList template-bean))
        target-source (.getExtTargetDs template-bean)
        target-dbHost (.getDbHost target-source)
        target-dbPort (.getDbPort target-source)
        target-sid (.getSid target-source)
        target-dbType (if (= "oracle" (.getDbType target-source))
                        (if (empty? target-sid)
                          (.getDbType target-source)
                          (str (.getDbType target-source) "-sid"))
                        (.getDbType target-source))
        target-dbName (if (= "1" resource-dept) (.getExtend1 template-bean) (.getDbName target-source))
        target-dbUser (.getDbUser target-source)
        target-dbPassword (.getDbPassword target-source)
        target-extend (.getExtend1 target-source)           ; host:port when es
        target-extend1 (.getExtend1 template-bean)          ;table fields when elasticsearch
        target-dsCharset (.getDsCharset target-source)
        target-table (.getTargetTableName template-bean)    ;type,index when elasticsearch
        today (util/format-date "yyyy-MM-dd")
        valid-time (first (clojure.string/split (.getValidTime template-bean) #"\s"))
        data-date (if (not= valid-time today)
                    (util/format-date (util/yesterday) "yyyy-MM-dd")
                    (first (clojure.string/split (.getDataTime template-bean) #"\s")))

        date-partition (if (empty? dt)
                         data-date
                         (.substring dt 0 10))]
    (doseq [source data-source]
      (let [dbHost (.getDbHost source)
            dbPort (.getDbPort source)
            sid (.getSid source)
            dbType (if (= "oracle" (.getDbType source))
                     (if (empty? sid)
                       (.getDbType source)
                       (str (.getDbType source) "-sid"))
                     (.getDbType source))
            dbName (.getDbName source)
            dbUser (.getDbUser source)
            _ (log/info "--------" dbHost "---------" dbName "-----------" dbUser)
            dbPassword (.getDbPassword source)
            dbExtend (.getExtend1 source)
            _ (log/info (str "dbExtend : " dbExtend))
            dsCharset (.getDsCharset source)
            dsEnable (.getDsEnable source)
            dsSqlStr (.getDsSqlStr source)
            dsWhereStr (.getDsWhereStr source)
            dsWhereStrEnable (.getDsWhereStrEnable source)
            _ (log/info dsWhereStr)
            source-sql (if (= "0" dsWhereStrEnable) dsSqlStr (str dsSqlStr " " dsWhereStr))
            final-sql (if (= "0" dsEnable) sql source-sql)
            _ (log/info "@@@@@@@@@ dbName :" dbName " dsEnable : " dsEnable " dsSql : " source-sql " final-sql : " final-sql)
            real-sqls (template/get-sqls dbType final-sql dbHost dbName data-date true dt)
            _ (log/info "real sqls : " real-sqls)]
        (doseq [real-sql real-sqls]
          (log/info "real-sql : " real-sql)
          (let [sub-job-id (generate-task-name ALBATROSS-SERVICE-NAME job-id (util/uuid))
                base-conf BASE-CONF
                sub-conf {:reader {:subprotocol dbType
                                   :host        dbHost
                                   :port        dbPort
                                   :database    dbName
                                   :user        dbUser
                                   :password    dbPassword
                                   :extend      dbExtend
                                   :charset     target-dsCharset
                                   :sql         real-sql
                                   :fetch-size  100}
                          :writer {:type              target-dbType
                                   :subprotocol       target-dbType
                                   :host              target-dbHost
                                   :port              target-dbPort
                                   :database          target-dbName
                                   :user              target-dbUser
                                   :password          target-dbPassword
                                   :tablename         target-table ;type,index when elas
                                   :extend            target-extend ;host:port when es
                                   :extend1           target-extend1 ;table fields when elas
                                   :compression-codec "lzo"
                                   :charset           dsCharset
                                   :delete-sql        delete-sql
                                   :dt                date-partition ;data-date
                                   :resource-dept     resource-dept}
                          }
                sub-job {sub-job-id (into base-conf sub-conf)}]
            (swap! sub-jobs conj sub-job)))))
    @sub-jobs))

(defn- generate-task-conf
  "return a vector: [task-confs task-num]"
  [job-id task-list]
  (reduce (fn [[tasks i] item]
            (let [task-id (generate-task-name ALBATROSS-SERVICE-NAME job-id i)
                  conf (into BASE-CONF item)]
              [(assoc tasks task-id conf) (inc i)]))
          [{} 1]
          task-list))

(defn- split-conf-to-list
  [template-bean job-id]
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
  [job-id]
  ; (strategy-fn (get-template-bean job-id) job-id)
  ; TODO 以下在测试时使用
  (let [conf-list (split-conf-to-list (template-bean->map (get-template-bean job-id)) job-id)]
    (map (fn [[task-id conf]]
           (let [ext-tar-ds (:extTargetDs conf)
                 target-table (:targetTableName conf)
                 sql (:sqlStr conf)
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
                       :password password}}))
         conf-list)))


(defn parse
  "return struct:
  strategy-fn return a map :
  {task-id [;; one thread in task one map
            {:source source :target target :sql sql :jar jar :klass klass :group group :type type}}"
  [job-id]
  ; (strategy-fn (get-template-bean job-id) job-id)
  ; TODO 以下在测试时使用
  (let [template-bean (get-template-bean job-id)]
    (log/info "config" template-bean)
    (log/info "config" (get-sub-job-configs template-bean job-id))
    (get-sub-job-configs template-bean job-id)))
