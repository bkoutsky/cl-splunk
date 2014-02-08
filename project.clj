(defproject cl-splunk "0.1.0-SNAPSHOT"
  :description "Clojure Splunk Library"
  :url "http://example.com/FIXME"
  :license {:name "BSD License"
            :file "LICENSE"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.google.code.gson/gson "2.1"]
                 [clj-yaml "0.4.0"]
                 [cheshire "5.2.0"]
                 [clj-time "0.5.1"]
                 [slingshot "0.10.3"]
                 [com.splunk/splunk "1.2.1.0"]]
  ;; comments below show how to add jars to local repo and how to use it
  ;; no longer needed as Splunk provides their own maven repo.
  ;; add jars to the local repo using
  ;; mvn install:install-file -Dfile=splunk-1.1.jar -DartifactId=splunk -Dversion=1.1 -DgroupId=splunk -Dpackaging=jar -DlocalRepositoryPath=repo  -DcreateChecksum=true
  ;; :repositories {"local" ~(str (.toURI (java.io.File. "repo")))}
  :repositories [["splunk" "http://splunk.artifactoryonline.com/splunk/ext-releases-local"]]
  :main cl-splunk
  :jvm-opts ["-Xmx8g"])
