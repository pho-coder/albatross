(ns com.jd.bdp.magpie.albatross.util.util
    (:require [clj-http.client :as client]
      [clojure.tools.logging :as logger]
      [clojure.data.json :as json]
      [sigmund.core :as sig]
      [clojure.java.io :as io])
    (:import (java.util Date Calendar Locale UUID)
      (org.apache.commons.lang StringUtils)
      (org.apache.commons.lang.time DateUtils)
      (java.text SimpleDateFormat)))

(defmacro >? [a b c d] `(if (> ~a ~b) ~c ~d))

(defmacro abs [x] `(if (< ~x 0) (- 0 ~x) ~x))

(defn isCheckThresholdOk?
      [nowData hisDatas limit]
      (let [validHisDatas (-> (sort hisDatas) rest butlast)
            avgHisDatas (/ (double (apply + validHisDatas)) (count validHisDatas))
            _ (logger/info (str "avgHisDatas : " avgHisDatas " nowData : " nowData))
            ratio (/ (double nowData) avgHisDatas)
            ratioGap (>? ratio 1 (dec ratio) (dec ratio))
            _ (logger/info (str "ratioGap between avgHisDatas and nowData is : " ratioGap))]
           ;(>? (abs ratioGap) (/ limit 100) [false avgHisDatas ratioGap] [true avgHisDatas ratioGap])
           (if (and (< ratioGap 0) (> (abs ratioGap) (/ limit 100)))
             [false avgHisDatas ratioGap]
             [true avgHisDatas ratioGap])
           ))

(defn read-over? [is-read-over]
      (true? (= 1 @is-read-over))
      )

(defn java-inc [^java.util.concurrent.atomic.AtomicInteger atomic-integer]
      (.incrementAndGet atomic-integer)
      )
(defn get-int [^java.util.concurrent.atomic.AtomicInteger atomic-integer]
      (.get atomic-integer))

(defn post-http-json
      [server-url form-params]
      (logger/debug (str server-url "?" form-params))
      (loop [loop-times 5]
            (let [result (try
                           (client/post server-url {:form-params form-params})
                           (catch Exception e
                             (logger/error (str "http ERROR, remaining number of retries: " loop-times))
                             (if (pos? loop-times) (Thread/sleep 1000))))]
                 (cond
                   (= (:status result) 200) (json/read-str (:body result)
                                                           :key-fn keyword)
                   (zero? loop-times) nil
                   true (recur (dec loop-times))))))

(defn get-http
      [server-url]
      (loop [loop-times 5]
            (let [result (try
                           (client/get server-url)
                           (catch Exception e
                             (println (str "http ERROR, remaining number of retries: " loop-times))
                             (if (pos? loop-times) (Thread/sleep 1000))))]
                 (cond
                   (= (:status result) 200) (:body result)
                   (zero? loop-times) nil
                   true (recur (dec loop-times))))))

(defn #^Date date
      [stamp]
      (Date. stamp)
      )
(defn #^Date now
      [] (Date.))

(defn #^Date yesterday
      [] (DateUtils/addDays (now) -1))

(defn #^String yesterday-str []
      (let [cal (Calendar/getInstance)
            _ (.add cal (Calendar/DATE) -1)
            dateNew (.getTime cal)
            formatter (SimpleDateFormat. "yyyyMMdd")]
           (.format formatter dateNew)))

(defn #^String format-date
      "format-date (date 121212122121) \"yyyy-MM-dd\""
      ([fm] (format-date (now) fm))
      ([d fm] (.format (SimpleDateFormat. fm) d))
      )

(defn get-conf
      []
      (if-let [plumber-conf (System/getenv "PLUMBER_CONF")]
              (do (logger/debug plumber-conf)
                  (if (.exists (io/file plumber-conf))
                    (json/read-str (slurp plumber-conf)
                                   :key-fn keyword)
                    (do (logger/error "plumber conf file not find!")
                        (System/exit 1))))
              (do (logger/error "$PLUMBER_CONF not find!")
                  (System/exit 1))))

(defn #^String get-date-hour [s fm]
      (let [ss (clojure.string/replace s #"\s" "")
            r (re-find (re-matcher #"^#(data-hour)(([+-])(\d+))?#$" ss))
            data-date (now)
            ]
           (if (or (nil? r) (not= (count r) 5))
             nil
             (cond
               (and (= "data-hour" (nth r 1)) (nil? (last r))) ;; #data-date#
               (format-date data-date fm)
               (and (= "data-hour" (nth r 1)) (= "-" (nth r 3))) ;;#data-date-10#
               (format-date (DateUtils/addHours data-date (- 0 (Integer/valueOf (last r)))) fm)
               :else nil))))

(def data-date-set #{"data-date" "data-date1" "data-date2"})

(defn #^String get-date [s & m]
      (let [ss (clojure.string/replace s #"\s" "")
            r (re-find (re-matcher #"^#((data-date)(\d){0,1})(([+-])(\d+))?#$" ss))
            dateStr (nth r 1)
            opt_pos 5                                       ;(if (= "data-date" dateStr) 3 5)
            plus_num (last r)
            _ (logger/info "dateStr:" dateStr "opt_pos:" opt_pos "plus_num:" plus_num)
            fm (or (second m) (cond (= "data-date1" dateStr) "yyyy/MM/dd"
                                    (= "data-date2" dateStr) "yyyyMMdd"
                                    (= "data-date" dateStr) "yyyy-MM-dd"
                                    :else nil)
                   )
            now (System/currentTimeMillis)
            data-date (if (= 0 (count m)) now (cond (= 10 (count (first m))) (.getTime (.parse (SimpleDateFormat. "yyyy-MM-dd") (first m)))
                                                    (= 8 (count (first m))) (.getTime (.parse (SimpleDateFormat. "yyyyMMdd") (first m)))
                                                    :else -1))
            ]
           (if (or (nil? r) (not= (count r) 7) (= -1 data-date))
             nil
             (cond
               (and (contains? data-date-set dateStr) (nil? (last r))) ;; #data-date#
               (format-date (date data-date) fm)
               (and (contains? data-date-set dateStr) (= "+" (nth r opt_pos))) ;;#data-date+10#
               (format-date (date (+ data-date (* (Integer/valueOf (last r)) 86400000))) fm)
               (and (contains? data-date-set dateStr) (= "-" (nth r opt_pos))) ;;#data-date+#
               (format-date (date (- data-date (* (Integer/valueOf (last r)) 86400000))) fm)
               :else nil))))

(defn conj-vectors
      [a-vector]
      "convert [[1] [2 3] [4 5] [5 6]] to [1 2 3 4 5 5 6]"
      (loop [new-vector [] a-vector a-vector]
            (if (= (count a-vector) 0)
              new-vector
              (recur (into new-vector (peek a-vector)) (pop a-vector)))))

(defn get-dec [refer]
      (dosync (alter refer dec)))

(defn get-inc [refer]
      (dosync (alter refer inc)))

(defn conj-data [useset data]
      (dosync (commute useset conj data)))

(defn- replace-vector [line field-term pattern pattarget nulltarget]
       (clojure.string/join field-term (map (fn [field] (if field (clojure.string/replace field pattern pattarget) nulltarget)) line)))

(defn- do-filter [line field-term line-term pattern pattarget nulltarget charactor]
       (.getBytes (str (replace-vector line field-term pattern pattarget nulltarget) line-term) charactor))

(defn create-leach-method [field-term line-term pattern pattarget nulltarget charactor]
      (fn [line] (do-filter line field-term line-term pattern pattarget nulltarget charactor)))

(defn create-leach [field-term line-term pattern pattarget nulltarget charactor]
      (fn [line] (str (replace-vector line field-term pattern pattarget nulltarget) line-term)))

(defn system-resources-enough?
      [jvm-mem-size]
      (let [mill (* 1024 1024)
            actual-free-memory (quot (:actual-free (sig/os-memory)) mill)
            swap (sig/os-swap)
            total-swap (quot (:total swap) mill)
            free-swap (quot (:free swap) mill)
            cpu-core (count (sig/cpu))
            load-avg (first (sig/os-load-avg))
            mem-enough (> actual-free-memory jvm-mem-size)
            ]
           (logger/info "free-memory(Mb):" actual-free-memory " total-swap(Mb):" total-swap " free-swap(Mb):" free-swap " load-avg:" load-avg " cpu-core:" cpu-core)
           (if mem-enough
             (> (* cpu-core 0.8) load-avg)
             (and (> (/ (- free-swap jvm-mem-size) total-swap) 0.6) (> (* cpu-core 0.8) load-avg)))))

(defn uuid []
      (str (UUID/randomUUID)))

(defn normalize-path
      "fix the path to normalized form"
      [path]
      (let [path (if (empty? path) "/" path)
            path (if (and (.endsWith path "/") (> (count path) 1))
                   (apply str (drop-last path))
                   path)]
           path))

(defn drop-last-while
      "Drop from last while pred is true"
      [pred coll]
      (loop [c coll]
            (let [tail (last c)]
                 (if (and (not= 0 (count c)) (pred tail))
                   (recur (drop-last c))
                   c))))

(defn parent
      "Get parent's path"
      [path]
      (cond
        (= path "/") "/"
        :default (let [path (if (.endsWith path "/") (drop-last path) path)]
                      (apply str (drop-last-while #(not= % \/) path)))))

(defn str-contains?
      [o-str search-str]
      (StringUtils/contains o-str search-str))

(defn rep-sql
      [sql]
      (clojure.string/replace (clojure.string/replace sql #"\s+|\t|\n" " ") #",\s+" ","))

(defn get-table-name
      [ss]
      (if (>= (count ss) 2)
        (if (= "from" (first ss))
          (second ss)
          (recur (rest ss)))
        "notable"))

(defn bytestohex
      [bytes-array]
      (let [mdfive-bytes bytes-array
            string-buffer (StringBuffer. (* (count mdfive-bytes) 2))
            length (count mdfive-bytes)
            md5-code (loop [i 0] (if (< i length)
                                   (do (.append string-buffer (Character/forDigit (bit-shift-right (bit-and (aget mdfive-bytes i) 240) 4) 16))
                                       (.append string-buffer (Character/forDigit (bit-and (aget mdfive-bytes i) 15) 16))
                                       (recur (inc i)))
                                   (.toString string-buffer)))]
           md5-code))


(defn -main
      [& args]
      ;(println (get-date "#data-date1+10#" "2014-10-12"))
      (println (yesterday))
      )
