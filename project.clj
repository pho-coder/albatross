(defproject com.jd.bdp.magpie/albatross "0.1.0.20151229-SNAPSHOT"
  :description "magpie plumber jobs sorter"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.3.1"]
                 [thrift-clj "0.3.0"]
                 [clj-zookeeper "0.2.0-SNAPSHOT"]
                 [com.jd.bdp.magpie/magpie-framework-clj "0.1.0-SNAPSHOT"]
                 [com.jd.bdp.magpie/magpie-utils "0.1.3-SNAPSHOT"]]
  :main com.jd.bdp.magpie.albatross.core
  :profiles {:uberjar {:aot [com.jd.bdp.magpie.albatross.core]}
             :dev {:dependencies [[com.taoensso/timbre "4.1.4"]]}}
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/java"]
  :test-paths ["test" "test"]
  :target-path "target"
  :plugins [[lein-thriftc "0.2.3"]
            [lein-kibit "0.1.2"]]
  :hooks [leiningen.thriftc]
  :thriftc {:path "thrift"
            :source-paths ["bin"]
            :target-path "target"
            :java-gen-opts "beans,hashcode,nocamel"
            :force-compile true})
