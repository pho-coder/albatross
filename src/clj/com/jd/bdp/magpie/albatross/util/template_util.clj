(ns com.jd.bdp.magpie.albatross.util.template-util
  (:require [clojure.tools.logging :as logger]
            [clojure.tools.logging :as log]
            [com.jd.bdp.magpie.albatross.util.util :as util]
            [com.jd.bdp.magpie.albatross.util.sql-parser-util :as sql-parser]))

(defn get-template-info
  [server-url template-id]
  (let [data (:data (util/post-http-json
                      server-url
                      {:content (str
                                  "{'token':'1','data':{templateId:'"
                                  template-id
                                  "'}}")}))]
    (if (or (= data nil) (= data []))
      (do (logger/error "template info data is nil or []!")
          (System/exit 1))
      (do (logger/debug (str "data info: " data))
          data))))

(defn put-sql-log
  [template-id a-sql-log]
  (let [sql-id (:sql-id a-sql-log)
        total-lines (:total-lines a-sql-log)
        start-time (:start-time a-sql-log)
        end-time (:end-time a-sql-log)
        node-id (:node-id a-sql-log)
        return-code (:code (util/post-http-json "http://buffalo.jd.com/restful/template/setSqlRunLog"
                                                {:content (str "{'token':'1','data':{templateId:'"
                                                               template-id "',templateSqlId:'"
                                                               sql-id "',nodeId:'"
                                                               node-id "',startTime:'"
                                                               start-time "',endTime:'"
                                                               end-time "',status:'1',execRows:'"
                                                               total-lines "',fileOutputPath:'/tmp/'}}")}))]
    (if (not= return-code "0")
      (logger/error "return sql run results error!"))))

(defn replace-data-date
  [sql data-date]
  (loop [rsql sql]
    (if-let [data-date-str (nth (re-find #"#((data-date)(\d){0,1})(([+-])(\d+))?#" rsql) 0)]
      (if-let [strs (re-find #"#@date##((data-date)(\d){0,1})(([+-])(\d+))?#" rsql)]
        (if (> (count strs) 0)
          (recur (clojure.string/replace (clojure.string/replace rsql data-date-str (util/get-date data-date-str data-date)) #"#@date#" ""))
          (recur (clojure.string/replace rsql data-date-str (str "'" (util/get-date data-date-str data-date) "'"))))
        (recur (clojure.string/replace rsql data-date-str (str "'" (util/get-date data-date-str data-date) "'"))))
      rsql)))

(defn get-length-str
  [length num]
  (let [num-str (str num)]
    (if (= 0 length)
      num-str
      (let [sub-len (- length (count num-str))]
        (if (> sub-len 0)
          (loop [x sub-len s num-str]
            (if (= 0 x)
              s
              (recur (- x 1) (str "0" s))))
          num-str)))))


(defn- verify-sql
  [sql]
  (let [step (re-find #"\$\d+\$" sql)
        jiou (re-find #"!\d!" sql)
        _ (logger/info "verify step:" step " jiou:" jiou)
        error-info "grammar error in your sql! divide-database-by-step and judge-odd-even are not supported at the same time!"]
    (if (and step jiou)
      (do (throw (RuntimeException. error-info)) (logger/error error-info)))
    ))

(defn- use-old-variable
  [sql]
  (if (or (not (nil? (nth (re-find #"#((data-date)(\d){0,1})(([+-])(\d+))?#" sql) 0)))
          (not (nil? (re-find #"@\d+@" sql)))
          (not (nil? (re-find #"!\d!" sql)))
          (not (nil? (re-find #"#\d+-\d+#" sql))))
    true
    false))

(defn- use-new-variable
  [sql]
  (if (or (not (nil? (re-find #"@(<\d+>)?(\d+-\d+)@" sql)))
          (not (nil? (re-find #"###[^#]*#" sql))))
    true
    false))

(defn get-real-sqls
  [o-sql ip db-name data-date isTarget date-str]
  (verify-sql o-sql)
  (let [srcdb-sql (if (= true isTarget)
                    (if (re-find #"@srcdb@" o-sql)
                      (clojure.string/replace o-sql "@srcdb@" db-name)
                      o-sql) o-sql)
        srcip-sql (if (= true isTarget)
                    (if (re-find #"@srcip@" srcdb-sql)
                      (clojure.string/replace srcdb-sql "@srcip@" (str "'" ip "'"))
                      srcdb-sql) srcdb-sql)
        ip-sql (if (= true isTarget)
                 (if (re-find #"@ip@" srcip-sql)
                   (clojure.string/replace srcip-sql "@ip@" (str "'" ip "_" db-name "'"))
                   srcip-sql) srcip-sql)
        use-old-variable (use-old-variable ip-sql)
        use-new-variable (use-new-variable ip-sql)
        _ (when (and use-old-variable use-new-variable)
            (log/error "mix old sql variable rules with new rules! sql : " o-sql)
            (System/exit 50557))
        final-sql-list (if (and use-old-variable (not use-new-variable))
                         (let [d-sql (replace-data-date ip-sql data-date)
                               [length l-sql] (if-let [result (re-find #"@\d+@" d-sql)]
                                                [(Integer/valueOf (second (clojure.string/split result #"@"))) (clojure.string/replace d-sql result "")]
                                                [0 d-sql]
                                                )
                               [jiou jo-sql] (if-let [result (re-find #"!\d!" l-sql)]
                                               [(Integer/valueOf (second (clojure.string/split result #"!"))) (clojure.string/replace l-sql result "")]
                                               [-1 l-sql]
                                               )
                               table-suffix (re-find #"#\d+-\d+#" jo-sql)]
                           (if table-suffix
                             (do (let [[n m] (clojure.string/split
                                               (clojure.string/replace table-suffix "#" "")
                                               #"-")
                                       stepStr (re-find #"\$\d+\$" jo-sql) ;$ is special in regrex
                                       step (if (nil? stepStr) 1 (Integer/valueOf (second (clojure.string/split stepStr #"\$"))))
                                       _ (logger/info "jo-sql:" jo-sql "\nstepStr:" stepStr)
                                       step-sql (if (nil? stepStr) jo-sql (clojure.string/replace jo-sql stepStr ""))
                                       _ (logger/info "step-sql:" step-sql)]
                                   (loop [start (Integer/valueOf n) r-sql []]
                                     (logger/info "n:" n "jiou:" jiou "r-sql:" r-sql)
                                     (if (> start (Integer/valueOf m))
                                       r-sql
                                       (if (< jiou 0)
                                         (recur (+ start step) (conj r-sql (clojure.string/replace (clojure.string/replace step-sql table-suffix (get-length-str length start)) "@id@" (String/valueOf start))))
                                         (if (= jiou (mod start 2))
                                           (recur (+ start 2) (conj r-sql (clojure.string/replace (clojure.string/replace step-sql table-suffix (get-length-str length start)) "@id@" (String/valueOf start))))
                                           (recur (inc start) r-sql ;(clojure.string/replace step-sql "@id@" (String/valueOf start))
                                                  )
                                           ))))))
                             [jo-sql]))
                         (if (and use-new-variable (not use-old-variable))
                                (sql-parser/parse-sql [ip-sql] date-str false)
                                [ip-sql]))]
    final-sql-list))

(defn get-real-sqls-back
  [o-sql ip db-name data-date]
  (let [ip-sql (if (re-find #"@ip@" o-sql)
                 (clojure.string/replace o-sql "@ip@" (str "'" ip "_" db-name "'"))
                 o-sql)
        sql (replace-data-date ip-sql data-date)
        table-suffix (re-find #"#\d+-\d+#" o-sql)]
    (if table-suffix
      (do (let [[n m] (clojure.string/split
                        (clojure.string/replace table-suffix "#" "")
                        #"-")]
            (loop [start (Integer/valueOf n) r-sql []]
              (if (> start (Integer/valueOf m))
                r-sql
                (recur (inc start) (conj r-sql (clojure.string/replace sql table-suffix (str start))))))))
      [sql])))


(defmulti get-sqls
          (fn [dbtype o-sql ip db-name data-date isTarget date-str] (str "" dbtype)))

(defmethod get-sqls "mysql"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "oracle"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "oracle-sid"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "sqlserver"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "hdfs"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "hive"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "elasticsearch"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "jingo"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "mongodb"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "hive_lzo"
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (get-real-sqls o-sql ip db-name data-date isTarget date-str))

(defmethod get-sqls "http"
  [dbtype o-sql ip db-name data-date isTarget date-str]     ;
  (let [deal-fn (fn [url] (let [str-list (clojure.string/split url #"#")]
                            (util/get-date-hour (str "#" (second str-list) "#") (nth str-list 2))))]
    (map deal-fn (clojure.string/split o-sql #"\|"))))

(defmethod get-sqls "hbase"                                 ;;temporary solution , call get-real-sqls instead.
  [dbtype o-sql ip db-name data-date isTarget date-str]
  (let [sql (ref [])
        sqlvec (conj @sql o-sql)]
    sqlvec))

(defn get-load-info
  [hadoop-path table-name data-date compression-codec local-file-path]
  {:hadoop-path hadoop-path :table-name table-name :data-date data-date :compression-codec compression-codec :local-file-path local-file-path})

(defn get-extract-info
  [extract-sql-info data-date]
  (let [info {:type     (:dbType (:dataSource extract-sql-info))
              :ip       (:dbHost (:dataSource extract-sql-info))
              :port     (:dbPort (:dataSource extract-sql-info))
              :user     (:dbUser (:dataSource extract-sql-info))
              :password (:dbPassword (:dataSource extract-sql-info))
              :db-name  (:dbName (:dataSource extract-sql-info))
              :sql-id   (:sqlId extract-sql-info)}
        id (atom 0)]
    (map #(conj info {:sql % :id (reset! id (+ @id 1))}) (get-real-sqls (:sqlStr extract-sql-info) (:ip info) (:db-name info) data-date))))

(defn get-sql-list
  [plumber-conf data-date template-id]
  (let [template-info (get (get-template-info (:server-url plumber-conf) template-id) 0)
        load-info (get-load-info (:hadoop-path plumber-conf)
                                 (:loadTableName (get (:loadSqlInfoList template-info) 0))
                                 data-date
                                 (if (= (:compressed template-info) 1)
                                   "lzo"
                                   "")
                                 (:local-file-path plumber-conf))
        extract-info (map #(get-extract-info % data-date) (:extractSqlInfoList template-info))]
    (vec (map (fn [a-sql] (vec (map #(conj load-info %) a-sql))) extract-info))))

(defn -main
  [& args]
  (let                                                      ;[sql "select a,b,@id@ from a_#0-10#"]
    [sql "select @srcip@,@srcdb@,@ip@,@id@,#0-10# from orderRelationInfo_@3@$3$#0-10#@<5>0001-0100@"
     sql2 "select * from orderRelationInfo_@3@##yyyyMM01:=-1M~yyyyMMdd:=+1# and t_@<4>001-050@"
     sql3 "select * from orderRelationInfo_!3!##yyyyMM01:=-1M~yyyyMMdd:=+1# and t_@<4>001-050@"
     sql4 "select * from orderRelationInfo_#0-10###yyyyMM01:=-1M~yyyyMMdd:=+1# and t_@<4>001-050@"
     sql5 "select * from orderRelationInfo_##yyyyMM01:=-1M~yyyyMMdd:=+1# and t_@<4>001-050@ where dt=#data-date-1#"
     sql6 "select peer-to-peer from t_##yyyyMM01:=-1M~yyyyMMdd:=+1# and t_@<4>001-010@"
     sql7 "select @srcip@,@srcdb@,@ip@,@id@ from t_##yyyy-MM-dd-HH-mm:=-2M~yyyy-MM-dd-HH-mm:=+5H# or ##yyyy/MM/dd HH:mm:=-50#"
     sql8 "select @srcip@,@srcdb@,@ip@,@id@,#0-10# from orderRelationInfo_@3@$3$#0-10#"
     sql9 "select @srcip@,@srcdb@,@ip@,@id@,#0-10# from orderRelationInfo"
     sql10 "select peer-to-peer from t_@<4>005-030@"
     sql1 "select @srcip@,@srcdb@,@ip@,@id@,#0-10# from orderRelationInfo_@3@$3$#0-10#@<5>0001-0100@ where dt=#data-date-1#"
	sql-tmp "select * from `t_1-#1-10#`"]
    
	(println (get-sqls "mysql" sql-tmp "192.168.1.1" "tmp" "2014-07-07" true ""))
    
    ;(println (get-sqls "mysql" sql "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql1 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql2 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql3 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql4 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql5 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql6 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql7 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql8 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql9 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    ;(println (get-sqls "mysql" sql10 "192.168.1.1" "tmp" "2015-07-21 12:20" true "2015-08-27 11:20"))
    
    ;(println (replace-data-date "select #@date##data-date1+10# from t" "2014-10-27"))
    ))
