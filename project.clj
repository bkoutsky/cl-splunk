(defproject cl-splunk "0.1.0-SNAPSHOT"
  :description "Clojure Splunk Library"
  :url "http://example.com/FIXME"
  :license {:name "BSD License"
            :file "LICENSE"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [splunk/splunk "1.1"]
                 [com.google.code.gson/gson "2.1"]
                 [cheshire "5.2.0"]
                 [clj-time "0.5.1"]
                 [slingshot "0.10.3"]]
  ;; add jars to the local repo using
  ;; mvn install:install-file -Dfile=splunk-1.1.jar -DartifactId=splunk -Dversion=1.1 -DgroupId=splunk -Dpackaging=jar -DlocalRepositoryPath=repo  -DcreateChecksum=true

  :repositories {"local" ~(str (.toURI (java.io.File. "repo")))}
  :main com.gooddata.cl-splunk
  :jvm-opts ["-Xmx 2g"])
