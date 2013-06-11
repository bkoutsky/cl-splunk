(ns com.gooddata.cl-splunk
  (:gen-class)
  (:use [slingshot.slingshot :only [try+ throw+]])
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [clojure.string :as string]
            [cheshire.core :as json]
            [cheshire.generate :refer [add-encoder encode-str remove-encoder]])
  (:import (com.splunk ServiceArgs Service JobExportArgs JobExportArgs$SearchMode JobExportArgs$OutputMode MultiResultsReaderXml MultiResultsReaderJson)
           (org.joda.time DateTime)
           (java.text SimpleDateFormat)
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
      (let [defaults (->> filename
                          clojure.java.io/reader
                          json/parse-stream)]
        (when-not (associative? defaults) (throw+ {:type ::config-not-a-map, :value filename}))
        (map #(defaults %) ["username" "password" "host" "port"]))
      [nil nil nil nil])))

(defn- connection-defaults
  [& args]
  (let [defaults-file (get-file-defaults)
        defaults-env (get-env-defaults)]
    (map #(some identity %&) args defaults-file defaults-env [nil nil nil 8089])))

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


(defn mktime
  "Convert anything to Splunk timespec, for certain definitions of \"anything\".
  Instance of org.joda.time.DateTime and java.util.Date is converted to unix time.
  Instances of Number and number-like Strings are assumed to be unix time.
  Other instances of String are assumed to be Splunk time-spec.
  Seq is assumed to be [year month day hour minute sec milisecond], any tail can be ommited."
  [time]
  (cond
   ;; Convert instance of org.joda.time.Datetime to UTC seconds
   (instance? Date time) (str (.getTime time))
   (instance? DateTime time) (str (time-coerce/to-long time))
   (instance? Number time) (str time)
   (and (instance? String time)
        (re-matches #"^\s*\d+(\.\d*)?\s*" time)) (string/trim time)
   (sequential? time) (-> (apply time/date-time time) time-coerce/to-long str)
   (instance? String time) time
   :else (throw+ {:type ::invalid-time, :value time})))



(def time-zones {"GMT" 0, "CEST" 2, "CET" 1, "PST" -8, "PDT" -7})
(def time-format (time-format/formatter nil "yyyy-MM-dd hh:mm:ss.SSS" "yyyy-MM-dd hh:mm:ss"))


(add-encoder DateTime encode-str)

(defn convert-date
  [[all ts tz-name]]
  (let [tz (-> tz-name time-zones time/time-zone-for-offset)
        time (time-format/parse time-format ts)]
    (time/from-time-zone time tz)))

;;; When adding extra patterns, make sure they contain
;;; at least one capturing group, even if not strictly necessary.
(def convert-patterns [[#"^\s*(\d+)\s*$" #(Long. (first %))]
                       [#"^\s*(\d*\.\d+)\s*$" #(Double. (first %))]
                       [#"^\s*(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)\s+([A-Z]+)?$" convert-date]
                       [#"^(.*)$" #(first %)]])

(defn convert-value
  [value]

  (first (for [[rx f] convert-patterns
               :let [match (first (re-seq rx value))]
               :when match]
           (f match))))

(defn convert-field
  [event key]
  (let [values (map convert-value (.getArray event key))]
    (if (= 1 (count values))
      (first values)
      values)))

(defn convert-event
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
               (.setEarliestTime (mktime from))
               (.setLatestTime (mktime to))
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
                    ;(json/generate-string opts)
                    prn)
               search))))

(defn -main
  [& args]
  (let [spl (connect)
        srch (export-search spl "search index=gdc sourcetype=erlang (gcf_event=\"new task\" OR gcf_event=\"task waiting\" OR gcf_event=\"task started\" OR gcf_event=\"task finished\" OR gcf_event=\"processing task\" OR gcf_event=\"task computed\") | table _time, task_id, request_id, task_type, gcf_event, time, project " "-0d@d+11h" "-0d@d+11h+10sec")]
    (search-to-json srch)

    (let [runtime (Runtime/getRuntime)
          free (/ (.freeMemory runtime) 1024 1024)
          total (/ (.totalMemory runtime) 1024 1024)]
      (println "Complete. Memory total " (int total) ", used " (int (- total free)) ", free " (int free)))))