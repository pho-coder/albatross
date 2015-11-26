(defproject com.jd.bdp.magpie/albatross "0.1.0.20151120-SNAPSHOT"
  :description "magpie plumber jobs sorter"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [thrift-clj "0.3.0"]
                 [ch.qos.logback/logback-classic "1.1.3"]
                 [com.jd.bdp.magpie/magpie-framework-clj "0.1.0-SNAPSHOT"]]
  :main com.jd.bdp.magpie.albatross.core
  :profiles {:uberjar {:aot :all}}
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/java"]
  :test-paths ["test" "test"]
  :target-path "target"
  :plugins [[lein-thriftc "0.2.3"]]
  :hooks [leiningen.thriftc]
  :thriftc {:path "thrift"
            :source-paths ["bin"]
            :target-path "target"
            :java-gen-opts "beans,hashcode,nocamel"
            :force-compile false})
