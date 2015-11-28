(ns com.jd.bdp.magpie.albatross.util.server-test
  (:require [clojure.test :refer :all]
            [com.jd.bdp.magpie.albatross.util.server :refer :all]))

(deftest start-server-test
  (testing
      (is (= true (start-server)))))
