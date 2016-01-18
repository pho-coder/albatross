(ns com.jd.bdp.magpie.albatross.util.time
  (:require [clojure.tools.logging :as log])
  (:import [java.util Calendar Date]
           [java.text SimpleDateFormat ParseException]))

(def default-pattern "yyyy-MM-dd HH:mm")
(def enum-year (Calendar/YEAR))
(def enum-month (Calendar/MONTH))
(def enum-day (Calendar/DATE))
(def enum-hour (Calendar/HOUR_OF_DAY))
(def enum-minute (Calendar/MINUTE))

(defn- yesterdayStr
  []
  (let [simpleDateFormat (SimpleDateFormat. default-pattern)
        calendar (Calendar/getInstance)
        _ (.setTime calendar (Date.))
        yesterday-int (dec (.get calendar (Calendar/DATE)))
        _ (.set calendar (Calendar/DATE) yesterday-int)
        _ (.set calendar (Calendar/HOUR_OF_DAY) 0)
        _ (.set calendar (Calendar/MINUTE) 0)
        time-long (.getTime calendar)]
    (.format simpleDateFormat time-long)))

(defn- internal-parse-date
  [oriDateStr pattern]
  (let [dateStr (if (or (nil? oriDateStr) (= (.length (.trim oriDateStr)) 0))
                  (yesterdayStr)
                  oriDateStr)
        calendar (Calendar/getInstance)
        simpleDateFormat (SimpleDateFormat. pattern)
        date (try
               (.parse simpleDateFormat dateStr)
               (catch ParseException e
                 (do
                   (log/error e)
                   (log/error (str "date-pattern : " pattern))
                   (throw (RuntimeException. e)))))
        _ (.setTime calendar date)]
    calendar))

(defn get-year
  [dateStr pattern]
  (let [calendar (internal-parse-date dateStr pattern)]
    (.get calendar (Calendar/YEAR))))

(defn get-month
  [dateStr pattern]
  (let [calendar (internal-parse-date dateStr pattern)]
    (inc (.get calendar (Calendar/MONTH)))))

(defn get-day
  [dateStr pattern]
  (let [calendar (internal-parse-date dateStr pattern)]
    (.get calendar (Calendar/DATE))))

(defn get-hour
  [dateStr pattern]
  (let [calendar (internal-parse-date dateStr pattern)]
    (.get calendar (Calendar/HOUR_OF_DAY))))

(defn get-minute
  [dateStr pattern]
  (let [calendar (internal-parse-date dateStr pattern)]
    (.get calendar (Calendar/MINUTE))))

(defn plus
  [dateStr field increment pattern]
  (let [calendar (Calendar/getInstance)
        simpleDateFormat (SimpleDateFormat. pattern)
        time-long (.getTime (.parse simpleDateFormat dateStr))
        _ (try
            (.setTimeInMillis calendar time-long)
            (catch ParseException e
              (do
                (log/error e)
                (log/error (str "date-pattern : " pattern))
                (throw (RuntimeException. e)))))
        new-field-val (+ (.get calendar field) increment)
        _ (.set calendar field new-field-val)]
    (.format simpleDateFormat (.getTime calendar))))

;(defn time->long
;  [date-str pattern]
;  )

