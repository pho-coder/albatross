(ns com.jd.bdp.magpie.albatross.conf-parser)

(defn parse
  "return struct: 
  ;; one task one map
  {task-id [;; one thread in task one map
            {:source source :target target :sql sql}]}"
  [job-id]
  {"test-task-0" (list {:source "mysql"
                        :target "hadoop"
                        :sql "select * from a"})
   "test-task-1" (list {:source "mysql"
                        :target "hadoop"
                        :sql "select * from b"})})
