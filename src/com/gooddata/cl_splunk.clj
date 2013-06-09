(ns com.gooddata.cl-splunk
  (:gen-class)
  (:use [slingshot.slingshot :only [throw+]])
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time-coerce]
            [clojure.string :as string]
            [cheshire.core :as json])
  (:import (com.splunk ServiceArgs Service JobExportArgs JobExportArgs$SearchMode MultiResultsReaderXml MultiResultsReaderJson)
           (org.joda.time DateTime)
           (java.text SimpleDateFormat)
           (java.util Date)))


;; (def ^:dynamic *splunk* nil)

(defn splunk-port [port]
  (cond
   port port
   (System/getenv "SPLUNK_PORT") (Integer. (System/getenv "SPLUNK_PORT"))
   :else 8089))

(defn splunk-host [host]
  (cond
   host host
   (System/getenv "SPLUNK_HOST") (System/getenv "SPLUNK_HOST")))


(defn connection-defaults
  [& args]
  (let [defaults_map (json/parse-stream (clojure.java.io/reader (str (System/getProperty "user.home") "/.splunk.json")))
        defaults_list (map #(defaults_map %) ["username" "password" "host" "port"])]
    (map #(some identity %&) args defaults_list [nil nil nil 8089])))


(defn connect
  ([] (apply connect (connection-defaults nil nil nil nil)))

  ([username password] (apply connect (connection-defaults username password nil nil)))

  ([username password host port]
   (let [args (doto (ServiceArgs.)
                (.setUsername username)
                (.setPassword password)
                (.setHost (splunk-host host))
                (.setPort (splunk-port port)))]
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
   :else time))

;;(let [x (time/date-time 1986 10 14 4 3 27 456)]
;;  (mktime "x12341234"))

(defn convert-value
  [value]
  (cond
   (re-matches #"\s*\d+\s*" value) (Long. (string/trim value))
   (re-matches #"\s*\d*\.\d+\s*" value) (Double. (string/trim value))
   ;; TODO: Parse time stamps
   :else value))

(defn key-value
  [event key]
  (let [values (map convert-value (.getArray event key))]
    (if (not= 1 (count values))
      values
      (first values))))

(defn convert-event
  "Converts Splunk's Event to sensible hashmap"
  [event]
  (let [fields (for [key (.keySet event)]
                 [key (key-value event key)])]
    (into {} fields)))

(defn read-multi-results [multi-reader]
  (for [reader multi-reader
        :when (not (.isPreview reader))
        event reader]
    (convert-event event)))

(defn export-search
  [splunk search from to]
  (let [args (doto (JobExportArgs.)
               (.setEarliestTime (mktime from))
               (.setLatestTime (mktime to))
               (.setSearchMode JobExportArgs$SearchMode/NORMAL))
        export (.export splunk search args)
        mrr (MultiResultsReaderXml. export)]
    (read-multi-results mrr)))

;;(defn print-events-from-rr [rr]
;;  (dorun (map #(print (str (.get % "_time") "," (.get % "method") "," (.get % "duration") "" \newline)) rr)))

;; (defn print-events-from-multi [mrr]
;;   (doseq [r mrr
;;           :when (not (.isPreview r))]
;;     (do
;;         (print-events-from-rr r))))



(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [spl (connect)
        srch (export-search spl "search index=gdc sourcetype=erlang (gcf_event=\"new task\" OR gcf_event=\"task waiting\" OR gcf_event=\"task started\" OR gcf_event=\"task finished\" OR gcf_event=\"processing task\" OR gcf_event=\"task computed\") | table _time, task_id, request_id, host, gcf_event, task_type, time" "-5min" "-4min")]
    (doall (map #(-> %
                     (json/generate-string {:pretty true})
                     println)
                srch))))
