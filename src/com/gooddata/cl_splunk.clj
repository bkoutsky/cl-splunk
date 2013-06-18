(ns com.gooddata.cl-splunk
  (:gen-class)
  (:use [slingshot.slingshot :only [try+ throw+]]
        clojure.java.io)
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [clojure.string :as string]
            [cheshire.core :as json])
  (:import (com.splunk ServiceArgs Service JobExportArgs JobExportArgs$SearchMode JobExportArgs$OutputMode MultiResultsReaderXml MultiResultsReaderJson)
           (org.joda.time DateTime)
           (java.util Date)
           (java.io File)))


;; (def ^:dynamic *splunk* nil)

(defn- get-env-defaults
  []
  (let [username (System/getenv "SPLUNK_USERNAME")
        password (System/getenv "SPLUNK_PASSWORD")
        host (System/getenv "SPLUNK_HOST")
        port (if-let [port (System/getenv "SPLUNK_PORT")]
               (Integer. (string/trim port))
               nil)]
    [username password host port]))

(defn- get-file-defaults
  []
  (let [filename (str (System/getProperty "user.home") "/.splunk.json")]
    (if (-> filename File. .isFile)
      (let [defaults (-> filename
                         clojure.java.io/reader
                         json/parse-stream)]
        (when-not (associative? defaults) (throw+ {:type ::config-not-a-map, :value filename}))
        (map defaults ["username" "password" "host" "port"]))
      [nil nil nil nil])))

(defn- connection-defaults
  [& args]
  (let [defaults-file (get-file-defaults)
        defaults-env (get-env-defaults)
        defaults-hardcoded [nil nil nil 8089]]
    (map (fn [& args] (some identity args)) args defaults-env defaults-file defaults-hardcoded)))

(defn connect
  ([] (apply connect (connection-defaults nil nil nil nil)))
  ([username password] (apply connect (connection-defaults username password nil nil)))
  ([username password host] (apply connect (connection-defaults username password host nil)))

  ([username password host port]
   (let [args (doto (ServiceArgs.)
                (.setUsername username)
                (.setPassword password)
                (.setHost host)
                (.setPort port))]
     (Service/connect args))))



(defn- time-to-ms
  "Guess if argument is UNIX time in seconds or milliseconds.
  Return UNIX time in milliseconds."
  [x]
  (let [t (if (< x 4133980800)
            (* 1000 x)
            x)]
    (long t)))

(defn- ms-to-s
  [ms]
  (let [integer (quot ms 1000)
        fraction (rem ms 1000)
        fraction (if (= fraction 0) "" (format ".%03d" fraction))]
    (str integer fraction)))

(defn- splunk-time
  "Convert anything to Splunk timespec, for certain definitions of \"anything\".
  Instances of org.joda.time.DateTime and java.util.Date are converted to unix time.
  Instances of Number and number-like Strings are assumed to be unix time.
  Other instances of String are assumed to be Splunk time-spec and are passed as-is.
  Seq is assumed to be [year month day hour minute sec milisecond] in current time zone, any tail can be ommited."
  [time]
  (cond
   ;; Convert instance of org.joda.time.Datetime to unix epoch milliseconds
   ;;; java.util.Date
   (instance? Date time) (-> (.getTime time) ms-to-s)
   ;;; org.jodatime.DateTime
   (instance? DateTime time) (-> time time-coerce/to-long ms-to-s)

   ;;; clojure.lang.Ratio
   (instance? clojure.lang.Ratio time) (str (ms-to-s (time-to-ms time)))
   ;;; other Numbers
   (instance? Number time) (ms-to-s (time-to-ms time))
   ;;; integer string
   (and (instance? String time)
        (re-matches #"^\s*\d+?\s*" time)) (-> time string/trim Long. time-to-ms ms-to-s)
   ;;; double string
   (and (instance? String time)
        (re-matches #"^\s*\d+(\.\d*)?\s*" time)) (-> time string/trim Double. time-to-ms ms-to-s)
   (sequential? time) (-> (apply time/date-time time)
                          (time/from-time-zone (time/default-time-zone))
                          time-coerce/to-long)
   (instance? String time) time
   :else (throw+ {:type ::invalid-time, :value time})))

(def tz-offsets [["GMT" 0]
                 ["UTC" 0]
                 ["CEST" 2]
                 ["CET" 1]
                 ["PST" -8]
                 ["PDT" -7]])

(def time-zones (into {}
                      (for [[name offset] tz-offsets]
                        [name (time/time-zone-for-offset offset)])))

(def time-format (time-format/formatter nil "yyyy-MM-dd HH:mm:ss.SSS" "yyyy-MM-dd HH:mm:ss"))

(defn- convert-date
  [[all ts tz-name]]
  (let [tz (time-zones tz-name)
        time (time-format/parse time-format ts)]
    (when-not tz (throw+ {:type ::unknown-time-zone :value tz-name}))
    (-> time
        (time/from-time-zone tz)
        time-coerce/to-long)))


;;; When adding extra patterns, make sure they contain
;;; at least one capturing group, even if not strictly necessary.
(def convert-patterns [[#"^\s*(\d+)\s*$" #(try+ (Long. (first %)) (catch NumberFormatException _ nil))]
                       [#"^\s*(\d*\.\d+)\s*$" #(try+ (Double. (first %)) (catch NumberFormatException _ nil))]
                       [#"^\s*(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)\s+([A-Z]+)?$" convert-date]])

(defn- convert-value
  [value]

  (if-let [new-value (first (for [[rx f] convert-patterns
                                  :let [match (first (re-seq rx value))]
                                  :when match
                                  :let [x (f match)]
                                  :when x]
                              x))]
    new-value
    value))


(defn- convert-field
  [event key]
  (let [values (map convert-value (.getArray event key))]
    (if (= 1 (count values))
      (first values)
      values)))

(defn- convert-event
  "Converts Splunk's Event to sensible hashmap"
  [event]
  (let [fields (for [key (.keySet event)]
                 [key (convert-field event key)])
        result (into {} fields)]
    result))

;; If you think that read-results and read-multi-results could be easily merged into
;; one function with a single (for) and no (concat), well, no.
;; It seems that there's some interference between (for)'s eager-laziness
;; and Splunk's handling of readers that results in weird random failures if you do this.
;; Trust me, I tried.

(defn read-results
  [reader]
  (doall (for [event reader]
           (convert-event event))))

(defn read-multi-results
  [multi-reader]
  (doall (for [reader (seq multi-reader)
               :when (not (.isPreview reader))]
           (read-results reader))))

(defn export-search
  [splunk search from to]
  (let [args (doto (JobExportArgs.)
               (.setEarliestTime (splunk-time from))
               (.setLatestTime (splunk-time to))
               (.setSearchMode JobExportArgs$SearchMode/NORMAL)
               (.setOutputMode JobExportArgs$OutputMode/JSON))
        export (.export splunk search args)
        multi-reader (MultiResultsReaderJson. export)
        results (read-multi-results multi-reader)]
    (apply concat results)))


(defn search-to-json
  ([search] (search-to-json search {}))
  ([search opts]
   (dorun (map #(-> %
                    (json/generate-string opts)
                    println)
               search))))

(defn -main
  [& args]
  (let [spl (connect)]

    (dorun (for [day (range 160 134 -1)
                 hour (range 0 24)]
             (let [from (format "-0y@y+%dd+%dh" day hour)
                   to (format "-0y@y+%dd+%dh" day (inc hour))
                   filename (format "dump-%d-%d.json" day hour)]
               (println "Generating from" from "to" to ", file" filename)
               (with-open [w (clojure.java.io/writer filename)]
                 (let [search (export-search spl
                                             "search index=gdc sourcetype=erlang (gcf_event=\"new task\" OR gcf_event=\"task waiting\" OR gcf_event=\"task started\" OR gcf_event=\"task finished\" OR gcf_event=\"processing task\" OR gcf_event=\"task computed\") | table _time, task_id, request_id, task_type, gcf_event, time, project, parse_time, insert_time, size, resolution, get_time, write_time, enqueue_time, wait_time, waiting_cnt, result, worker_pid"
                                             ;"search index=gdc sourcetype=erlang (gcf_event=\"new task\" OR gcf_event=\"task waiting\" OR gcf_event=\"task started\" OR gcf_event=\"task finished\" OR gcf_event=\"processing task\" OR gcf_event=\"task computed\") | stats count by task_type "
                                             from
                                             to)]
                   (dorun (for [event search]
                            (.write w (str (json/generate-string event) "\n")))))))))

    (let [runtime (Runtime/getRuntime)
          free (/ (.freeMemory runtime) 1024 1024)
          total (/ (.totalMemory runtime) 1024 1024)]
      (println "Complete. Memory total " (int total) ", used " (int (- total free)) ", free " (int free)))))