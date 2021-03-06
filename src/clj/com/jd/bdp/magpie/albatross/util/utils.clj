(ns com.jd.bdp.magpie.albatross.util.utils
  (:require [clojure.tools.logging :as log]
            [clj-zookeeper.zookeeper :as zk]
            [com.jd.bdp.magpie.util.utils :as magpie-utils])
  (:import [org.apache.zookeeper KeeperException$NodeExistsException KeeperException$NoNodeException]))

(defn zk-new-client
  [zk-str]
  (zk/new-client zk-str))

(defn zk-close
  []
  (zk/close))

(defn create-albatross-node
  [albatross-node]
  (try
    (zk/create albatross-node :mode :ephemeral)
    true
    (catch Exception e
      (if (= (.getClass e) KeeperException$NodeExistsException)
        false
        (do (log/error e)
            (throw e))))))

(defn set-albatross-info
  [albatross-node info]
  (try
    (zk/set-data albatross-node (magpie-utils/map->bytes info))
    (catch Exception e
      (if (= (.getClass e) KeeperException$NoNodeException)
        (do (zk/create albatross-node :mode :ephemeral)
            (zk/set-data albatross-node (magpie-utils/map->bytes info)))
        (do (log/error e)
            (throw e))))))

(defn get-nimbus
  [nimbus-path]
  (let [nodes (zk/get-children nimbus-path)
        size (.size nodes)]
    (when (>= size 1)
      (let [nimbuses (to-array nodes)
            _ (java.util.Arrays/sort nimbuses)
            nimbus (first nimbuses)]
        (magpie-utils/bytes->map
         (zk/get-data (str nimbus-path "/" nimbus)))))))

(defn check-magpie-task-exists?
  [task-id]
  (let [tasks-path "/assignments/"
        task-node (str tasks-path task-id)]
    (zk/check-exists? task-node)))

(defn get-job-info
  [job-node]
  (let [job-info (zk/get-data job-node)]
    (when-not (nil? job-info) (magpie-utils/bytes->map job-info))))

(defn create-job-node
  [job-node]
  (try
    (zk/create job-node :mode :ephemeral)
    true
    (catch Exception e
      (log/error e)
      false)))

(defn set-job-info
  [job-node info]
  (try
    (zk/set-data job-node (magpie-utils/map->bytes info))
    true
    (catch Exception e
      (log/error e)
      false)))

(defn register-job
  [job-node job-info]
  (if (zk/check-exists? job-node)
    (do (log/warn job-node "EXISTS!")
        false)
    (if (create-job-node job-node)
      (set-job-info job-node job-info)
      false)))

(defn delete-job-node
  [job-node]
  (try
    (if (zk/check-exists? job-node)
      (do (zk/delete job-node)
          true)
      true)
    (catch Exception e
      (log/error e)
      false)))
