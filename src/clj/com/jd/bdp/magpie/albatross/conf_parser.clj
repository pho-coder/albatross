(ns com.jd.bdp.magpie.albatross.conf-parser
  (:require [clojure.tools.logging :as log])
  (:import (com.jd.extservice.impl PlumberServiceImpl)
           (com.jd.exception BuffaloException)))

(defn- template-bean->map
  [abean]
  (let [amap (bean abean)
        ext-target-ds (bean (:extTargetDs amap))
        ext-src-ds-list (map bean (:extSrcDsList amap))]
    (assoc amap :extTargetDs ext-target-ds :extSrcDsList ext-src-ds-list)))

(defn- get-template-bean
  [template-id]
  (let [ps (PlumberServiceImpl.)
        template-bean (try
                        (.getTemplateById ps template-id)
                        (catch BuffaloException e (.printStackTrace e) (log/error e) nil))]
    (template-bean->map template-bean)))

(defn parse
  "return struct: 
  ;; one task one map
  {task-id [;; one thread in task one map
            {:source source :target target :sql sql}]}"
  [job-id]
  (get-template-bean job-id)

  ;临时返回的测试值
  {"test-task-0" (list {:source "mysql"
                        :target "hadoop"
                        :sql "select * from a"})
   "test-task-1" (list {:source "mysql"
                        :target "hadoop"
                        :sql "select * from b"})})
