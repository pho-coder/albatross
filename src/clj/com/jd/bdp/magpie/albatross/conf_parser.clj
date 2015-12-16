(ns com.jd.bdp.magpie.albatross.conf-parser
  (:require [clojure.tools.logging :as log])
  (:import (com.jd.extservice.impl PlumberServiceImpl)
           (com.jd.exception BuffaloException)))

(defn template-bean->map
  ; 因为bean方法不能迭代地执行，把JavaBean内部的JavaBean变成 Clojure Map
  ; 没有找到合适的方法，只好用原始手段构造
  [abean]
  (let [amap (bean abean)
        ext-target-ds (bean (:extTargetDs amap))
        ext-src-ds-list (map bean (:extSrcDsList amap))]
    (assoc amap :extTargetDs ext-target-ds :extSrcDsList ext-src-ds-list)))

(defn get-template-bean
  [template-id]
  (let [ps (PlumberServiceImpl.)
        template-bean (try
                        (.getTemplateById ps template-id)
                        (catch BuffaloException e (.printStackTrace e) (log/error e) nil))]
    (template-bean->map template-bean)))

(defn- default-split-fn
  "默认拆分方法：拆分任务为多个子任务，按数据源分"
  [template-bean job-id]
  (let [extSrcDsList (:extSrcDsList template-bean)
        base-bean (assoc template-bean :extSrcDsList [])
        task-list (map (fn [item]
                         (assoc base-bean :extSrcDsList [item]))
                       extSrcDsList)]
    (reduce (fn [[tasks i] item]
              [(assoc tasks (keyword (str job-id "-" i)) item) (inc i)])
            [{} 1]
            task-list)))

(defn parse
  "return struct: 
  ;; one task one map
  {task-id [;; one thread in task one map
            {:source source :target target :sql sql}]}"
  [job-id & {:keys [strategy-fn]
             :or {strategy-fn default-split-fn}}]
  (let [abean (strategy-fn (get-template-bean job-id) job-id)]
    #_(clojure.pprint/pprint abean))

  ;临时返回的测试值
  {"test-task-0" (list {:source "mysql"
                        :target "hadoop"
                        :sql "select * from a"})
   "test-task-1" (list {:source "mysql"
                        :target "hadoop"
                        :sql "select * from b"})})
