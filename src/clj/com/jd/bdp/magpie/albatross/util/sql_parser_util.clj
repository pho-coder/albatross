(ns com.jd.bdp.magpie.albatross.util.sql-parser-util
  (:require [clojure.string :as str]
            [com.jd.bdp.magpie.albatross.util.time :as time]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

(def year-format #{"yyyy"})
(def month-format #{"MM"})
(def day-format #{"dd"})
(def hour-format #{"HH"})
(def minute-format #{"mm"})

(defn- standard-format
  [date-str]
  (let [r (re-find (re-matcher #"^([y0-9]{4})([^A-Za-z0-9])*([M0-9]{2})([^A-Za-z0-9])*([d0-9]{2})?([^A-Za-z0-9])*([H0-9]{2})?([^A-Za-z0-9])*([m0-9]{2})?" date-str))
        _ (println "r : " r)
        year (nth r 1)
        month (nth r 3)
        day (nth r 5)
        hour (nth r 7)
        minute (nth r 9)
        yyyy-str (if (not (nil? year))
                   (if (contains? year-format year)
                     date-str
                     (str/replace-first date-str year "yyyy"))
                   date-str)
        MM-str (if (not (nil? month))
                 (if (contains? month-format month)
                   yyyy-str
                   (str/replace-first yyyy-str month "MM"))
                 yyyy-str)
        dd-str (if (not (nil? day))
                 (if (contains? day-format day)
                   MM-str
                   (str/replace-first MM-str day "dd"))
                 MM-str)
        HH-str (if (not (nil? hour))
                 (if (contains? hour-format hour)
                   dd-str
                   (str/replace-first dd-str hour "HH"))
                 dd-str)
        standard-date-str (if (not (nil? minute))
                            (if (contains? minute-format minute)
                              HH-str
                              (str/replace-first HH-str minute "mm"))
                            HH-str)]
    standard-date-str))

(defn- verify-range-format
  [begin end]
  (when (not= (standard-format (first (str/split begin #":="))) (standard-format (first (str/split end #":="))))
    (log/error "begin date format is different with end date format!")
    (System/exit 50556)))

(defn- normalize-field
  [field]
  (if (< field 10)
    (str "0" field)
    (String/valueOf field)))

(defn- compute-plus
  [date-str increment real-pattern]
  (let [incre-val (if (Character/isDigit (.charAt increment (dec (.length increment))))
                    (str increment "d")
                    increment)
        last-index-of-increment (dec (.length incre-val))
        num (Integer/valueOf (.substring incre-val 0 last-index-of-increment))
        field (.charAt incre-val last-index-of-increment)
        _ (println (str "incre-val : " incre-val " num : " num " field : " field " real-pattern : " real-pattern))]
    (cond
      (= \y field) (time/plus date-str time/enum-year num real-pattern)
      (= \M field) (time/plus date-str time/enum-month num real-pattern)
      (= \d field) (time/plus date-str time/enum-day num real-pattern)
      (= \H field) (time/plus date-str time/enum-hour num real-pattern)
      (= \m field) (time/plus date-str time/enum-minute num real-pattern)
      :else nil)))                                          ;nil proper?

(defn- internal-replace-date
  [time-expr date-str use-split-table-old]
  (let [equal-index (.indexOf time-expr "=")
        date-pattern (if (= -1 equal-index)
                       (.substring time-expr 0 (dec (.length time-expr)))
                       (.substring time-expr 0 (dec (.indexOf time-expr "="))))
        date-to-compute (-> time-expr
                            (.replaceAll "yyyy" (String/valueOf (time/get-year date-str time/default-pattern)))
                            (.replaceAll "MM" (normalize-field (time/get-month date-str time/default-pattern)))
                            (.replaceAll "dd" (normalize-field (time/get-day date-str time/default-pattern)))
                            (.replaceAll "HH" (normalize-field (time/get-hour date-str time/default-pattern)))
                            (.replaceAll "mm" (normalize-field (time/get-minute date-str time/default-pattern)))
                            )
        _ (log/info "date to compute : " date-to-compute)
        _ (log/info "date-pattern : " date-pattern)
        standard-format (standard-format date-pattern)
        compute-range (str/split date-to-compute #"~")
        _ (when (> (count compute-range) 1)
            (println (str "!!" (first compute-range) " " (second compute-range)))
            (verify-range-format (first compute-range) (second compute-range)))
        _ (when (and use-split-table-old (> (count compute-range) 1))
            (log/error "mix old split-table rules with new rules together !")
            (System/exit 50555))
        builder (StringBuilder.)
        _ (doseq [part compute-range]
            (let [compute-matcher (re-seq #":=(([+-])(\d+))?([y|M|d|H|m])?" part)
                  _ (if (nil? compute-matcher)
                      (.append builder part)
                      (let [date-str (first (str/split part #":="))
                            _ (println (str "part : " part " date-str : " date-str " compute-matcher : " compute-matcher))
                            increment (str/replace (first (first compute-matcher)) #":=" "")
                            _ (println (str "increment : " increment))
                            _ (println (str "@@" (compute-plus date-str increment standard-format)))]
                        (.append builder (compute-plus date-str increment standard-format)))) ;find only one compute expr
                  _ (.append builder "~")]))
        _ (.deleteCharAt builder (dec (.length builder)))]
    [(.toString builder) standard-format]))

(defn- get-final-date-field
  [pre-date]
  (let [pre-date-format (standard-format pre-date)
        pre-date-format-count (count (str/replace pre-date-format #"[^yMdHm]+" ""))
        date-field (cond
                     (= 12 pre-date-format-count) time/enum-minute
                     (= 10 pre-date-format-count) time/enum-hour
                     (= 8 pre-date-format-count) time/enum-day
                     (= 6 pre-date-format-count) time/enum-month
                     (= 4 pre-date-format-count) time/enum-year
                     :else nil)]
    date-field))

(defn- get-length-str
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


(defn- internal-replace-sub-sql
  [rsql]
  (let [table-suffix (re-find (re-matcher #"@(<\d+>)?(\d+-\d+)@" rsql))
        _ (println "table-suffix : " table-suffix)
        replace-suffix (first table-suffix)]
    (if table-suffix
      (do (let [original-step (nth table-suffix 1)
                _ (println original-step)
                [n m] (str/split (nth table-suffix 2) #"-")
                _ (println "n : " n " ; m : " m)
                real-step (if (= nil original-step)
                            (Integer/valueOf "1")
                            (do (let [rsql (clojure.string/replace rsql original-step "")
                                      _ (log/info rsql)]
                                  (Integer/valueOf (clojure.string/replace original-step #"[<>]" "")))))
                _ (println real-step)
                ]
            (loop [start (Integer/valueOf n) r-sql []]
              (if (> start (Integer/valueOf m))
                (do (log/info "sub table list : " r-sql) r-sql)
                (recur (+ start real-step) (conj r-sql (clojure.string/replace rsql replace-suffix (get-length-str (if (> (count n) (count m)) (count n) (count m)) start))))))))
      [rsql])))


(defn parse-sql
  [sql-list date-str use-split-table-old]
  (let [final-sql-list (ref [])
        _ (doseq [sql sql-list]
            (let [ref-sql (ref sql)
                  time-matchers (re-seq #"###[^#]*#" @ref-sql)
                  time-spilt-table (ref [])
                  _ (doseq [time-expr time-matchers]
                      (let [replace-result (internal-replace-date (str/replace time-expr #"#" "") date-str use-split-table-old)
                            _ (println (str (first replace-result) "  " (second replace-result)))
                            _ (println (str "time-expr : " time-expr))
                            _ (if (not= -1 (.indexOf time-expr "~"))
                                (do
                                  (dosync (commute time-spilt-table conj true)) ; if range
                                  (dosync (commute time-spilt-table conj (first replace-result))) ; range expr
                                  (dosync (commute time-spilt-table conj (second replace-result)))) ;standard format
                                (dosync (commute time-spilt-table conj false)))]
                        (dosync (ref-set ref-sql (str/replace @ref-sql time-expr (first replace-result))))))
                  _ (println (str "time-spilt-table : " @time-spilt-table))
                  ; split table by time
                  time-sql-list (if (first @time-spilt-table)
                                  (let [[n m] (str/split (second @time-spilt-table) #"~")
                                        end-time (Long/valueOf (str/replace m #"(\D+)" ""))
                                        field (get-final-date-field n)
                                        step 1              ;in case more than 1 some day..
                                        real-pattern (nth @time-spilt-table 2)]
                                    (loop [start n r-sql []]
                                      (if (= (Long/valueOf (str/replace start #"(\D+)" "")) end-time)
                                        (do (log/info "final time sql list : " r-sql) r-sql)
                                        (let [new-date (time/plus start field step real-pattern)
                                              new-sql (str/replace @ref-sql (second @time-spilt-table) new-date)]
                                          (recur new-date (conj r-sql new-sql))))))
                                  [@ref-sql])
                  _ (println (str "time-sql-list : " (first time-sql-list)))
                  ; split table by sql
                  _ (doseq [time-sql time-sql-list]
                      (let [_ (println (str "time-sql : " time-sql))
                            sub-sql-list (internal-replace-sub-sql time-sql)]
                        (dosync (commute final-sql-list into sub-sql-list))))]))]
    @final-sql-list))

(defn -main
  [& args]
  ;(print (parse-sql ["select peer-to-peer from t_#yyyyMM01:=-1M~yyyyMMdd:=+1# and t_@<4>001-050@"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_#yyyyMM01:=-1M~yyyyMMdd:=+1#"] "2015-07-21 12:20" false))
  (print (parse-sql ["select peer-to-peer from t_###yyyyMM01:=-1M~yyyyMMdd:=+1#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from where createDate > #yyyy-MM-dd HH:mm:=-5M# and createDate <#yyyyMMddHHmm:=+2#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_#yyyy-MM-dd HH:mm:=-5d~yyyy-MM-dd HH:mm:=+50m#"] "2015-07-21 12:20" false))  
  ;(print (parse-sql ["select peer-to-peer from t_#yyyy-MM-dd HH:mm:=-5d~yyyy-MM-ddHHmm:=+50m#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t where (createDate > #yyyyMMddHHmm:=-5# and createDate <#yyyyMMdd#) or (createDate > #yyyyMMdd:=-1M# and createDate <#yyyyMMdd:=+2y#)"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_#yyyy-MM-dd-HH-mm:=-2M~yyyy-MM-dd-HH-mm:=+5# or #yyyyMMdd:=-50#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_#yyyy-MM-dd-HH-mm:=-2M~yyyy-MM-dd-HH-mm:=+5H# or #yyyy/MM/dd HH:mm:=-50#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_#yyyyMMdd HH:mm:=-1y~yyyyMMddHH00#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_#yyyy:=-1y#"] "2015-07-21 12:20" false))
  ;(print (parse-sql ["select peer-to-peer from t_@<5>0001-0100@"] "2015-07-21 12:20" false))
  )
