(ns cl-splunk
  (:gen-class)
  (:use [slingshot.slingshot :only [try+ throw+]]
        clojure.java.io)
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [clojure.string :as string]
            [cheshire.core :as json]
            [clj-yaml.core :as yaml])
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
  (let [filename (str (System/getProperty "user.home") "/.splunk.yaml")]
    (if (-> filename File. .isFile)
      (let [defaults (-> filename
                         slurp
                         yaml/parse-string)]
        (when-not (associative? defaults) (throw+ {:type ::config-not-a-map, :value filename}))
        (map defaults [:username :password :host :port]))
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
  "Convert anything to Splunk timespec, for certain definition of \"anything\".
  Instances of org.joda.time.DateTime and java.util.Date are converted to unix time.
  Instances of Number and number-like Strings are assumed to be unix time, possibly in ms.
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
                          time-coerce/to-long
                          ms-to-s)
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
  ;;(let [values (map convert-value (.getArray event key))]
  (let [values (.getArray event key)]
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

(defn run-query [spl from to filename]
  (println "Generating from" from "to" to ", file" filename)
  (try+
   (with-open [w (clojure.java.io/writer filename)]
     (binding [*out* w]
       (let [search (export-search spl
                                   "search sourcetype=erlang gcf_event=* "
                                   from
                                   to)]
         (doseq [event search
                 :let [event (apply merge (map (fn [[k v]] {(keyword (.toLowerCase k)) v}) event))]]
           (prn event)))))
   (catch Object e
     (println "error in " from ": " (str e) " " (.getMessage e))))
  (println "Finished " from)
  spl)

(def THREADS 6)

(defn -main
  [& args]
  (println "Starting up")
  (let [splunk (connect)
        agents (vec (map (fn [_] (agent splunk :error-mode :continue)) (range 0 THREADS)))]
    (println "agents created")
;;     (doseq [day (range 0 31)
;;             hour (range 0 24)
;;             minute (range 0 60 10)]
    (doseq [day (range 0 1)
            hour (range 0 1)
            minute (range 0 60 10)]

      (let [from (format "-1month@month+%dd@d+%dh+%dmin" day hour minute)
            to (format "-1month@month+%dd@d+%dh+%dmin" day hour (+ minute 10))
            filename (format "data/dump-%02d-%02d-%02d" day hour minute)]
        (send-off (agents (quot minute 10)) run-query from to filename)))

    (apply await agents)

    (let [runtime (Runtime/getRuntime)
          free (/ (.freeMemory runtime) 1024 1024)
          total (/ (.totalMemory runtime) 1024 1024)]
      (println "Complete. Memory total " (int total) ", used " (int (- total free)) ", free " (int free)))
    (shutdown-agents)))
