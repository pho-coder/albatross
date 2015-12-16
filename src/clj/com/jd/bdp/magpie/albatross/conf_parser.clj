(ns com.jd.bdp.magpie.albatross.conf-parser)

(defn parse
  "return struct: 
  ;; one task one map
  {task-id ;; one thread in task one map
            {:source source :target target :sql sql :jar jar :klass klass :group group :type type}}"
  [job-id]
  {"plumber*p*albatross-test-0*p*test-job*p*task-0" {:source "mysql"
                                                     :target "hadoop"
                                                     :sql "select * from a"
                                                     :jar "magpie-test-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                                                     :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_test_plumber_task.core"
                                                     :group "default"
                                                     :type "memory"}
   "plumber*p*albatross-test-0*p*test-job*p*task-1" {:source "mysql"
                                                     :target "hadoop"
                                                     :sql "select * from b"
                                                     :jar "magpie-test-plumber-task-0.0.1-SNAPSHOT-standalone.jar"
                                                     :klass "com.jd.bdp.magpie.magpie_eggs_clj.magpie_test_plumber_task.core"
                                                     :group "default"
                                                     :type "memory"}})
