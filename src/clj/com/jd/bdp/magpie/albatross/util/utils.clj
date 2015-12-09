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
    (if (< size 1)
      nil
      (let [nimbuses (to-array nodes)
            _ (java.util.Arrays/sort nimbuses)
            nimbus (first nimbuses)]
        (magpie-utils/bytes->map (zk/get-data (str nimbus-path "/" nimbus)))))))
