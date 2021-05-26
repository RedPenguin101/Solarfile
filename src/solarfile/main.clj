(ns solarfile.main
  (:require [solarfile.server :refer [start]])
  (:gen-class))

(defn -main []
  (start))