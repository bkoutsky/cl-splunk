(ns com.gooddata.cl-splunk-test
  (:use com.gooddata.cl-splunk
        clojure.test)
  (:require clj-time.core)
  (:import (java.util Date)))


(defmacro with-private-fns [[ns fns] & tests]
  "Refers private fns from ns and runs tests in context."
  `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
     ~@tests))

(with-private-fns [com.gooddata.cl-splunk [splunk-time time-to-ms ms-to-s convert-value]]
  (deftest splunk-time-test
    (testing "from java.util.date"
      (is (= (splunk-time (Date. 1371384000123))
             "1371384000.123" )))
    (testing "from clj-time"
      (is (= (splunk-time (clj-time.core/date-time 2013 6 16 12 0 0 123))
             "1371384000.123")))
    (testing "from Ratio in seconds"
      (is (= (splunk-time (/ 1371384000123 1000))
             "1371384000.123")))
    (testing "from Number in seconds"
      (is (= (splunk-time 1371384000.123)
             "1371384000.123")))
    (testing "from Number in miliseconds"
      (is (= (splunk-time 1371384000123)
             "1371384000.123")))
    (testing "from String in miliseconds"
      (is (= (splunk-time "1371384000123")
             "1371384000.123")))
    (testing "from float String in seconds"
      (is (= (splunk-time "1371384000.123")
             "1371384000.123")))
    (testing "from integer String in seconds"
      (is (= (splunk-time "1371384000")
             "1371384000")))
    (testing "from non-numeric string"
      (is (= (splunk-time "2d@d")
             "2d@d")))
    (testing "from [y m d] sequence"
      (is (= (splunk-time [2013 6 20])
             "1371679200")))
    (testing "from [y m d h m] sequence"
      (is (= (splunk-time [2013 6 20 13 30])
             "1371727800")))
    (testing "from [y m d h m s] sequence"
      (is (= (splunk-time [2013 6 20 13 30 11])
             "1371727811")))
    (testing "from [y m d h m s] sequence"
      (is (= (splunk-time [2013 6 20 13 30 11 543])
             "1371727811.543")))

    )

  (deftest time-to-ms-test
    (testing "time in seconds"
      (is (= (time-to-ms 1371384000)
             1371384000000)))
    (testing "time in miliseconds"
      (is (= (time-to-ms 1371384000001)
             1371384000001))))

  (deftest ms-to-s
    (testing "fractional"
      (is (= (ms-to-s 1371384000001)
             "1371384000.001")))
    (testing "integer"
      (is (= (ms-to-s 1371384000000)
             "1371384000"))))

  (deftest convert-value
    (testing "CEST date"
      (is (= (convert-value "2013-07-16 13:23:01 CEST")
             1373973781000)))
    (testing "GMT date"
      (is (= (convert-value "2013-07-16 13:23:01 GMT")
             1373980981000)))
    (testing "Long"
      (let [r (convert-value "1234567")]
        (is (= r 1234567))
        (is (= (class r) Long))))
    (testing "Double"
      (let [r (convert-value "1234567.2")]
        (is (= r 1234567.2))
        (is (= (class r) Double))))
    (testing "some string"
      (is (= (convert-value "inconcievable") "inconcievable")))))











