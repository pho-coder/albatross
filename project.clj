(defproject com.jd.bdp.magpie/albatross "0.1.0.20151120-SNAPSHOT"
  :description "magpie plumber jobs sorter"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.thrift/libthrift "0.9.3"]
                 [com.jd.bdp.magpie/magpie-framework-clj "0.1.0-SNAPSHOT"]]
  :main com.jd.bdp.magpie.albatross.core
  :profiles {:uberjar {:aot :all}}
  :source-paths ["src" "src/clj"]
  :java-source-paths ["src/java"])
