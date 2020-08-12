(ns task-view.core
  (:require-macros [cljs.core.async.macros :refer [go]])
  (:require
    [cljs.core.async :as a :refer [<! >!]]
    [haslett.client :as ws]
    [haslett.format :as fmt]
    [vertx3-eventbus-client :as EventBus]
    [ajax.core :refer [GET POST]]
    [reagent.core :as reagent]
    [reagent.dom :as rdom]
    [re-frame.core :as re-frame]
    [task-view.events :as events]
    [task-view.routes :as routes]
    [task-view.views :as views]
    [task-view.config :as config]))



(defn dev-setup []
  (when config/debug?
    (println "dev mode")))

(defn ^:dev/after-load mount-root []
  (re-frame/clear-subscription-cache!)
  (let [root-el (.getElementById js/document "app")]
    (rdom/unmount-component-at-node root-el)
    (rdom/render [views/main-panel] root-el)))

#_(go (let [stream (<! (ws/connect "ws://localhost:8080/"))]
        (>! (:sink stream) "Hello World")
        (js/console.log (<! (:source stream)))
        (ws/close stream)))


(defn init []
  (let [eb (EventBus. "/events")]
    ;(GET "/events" (:handler #(prn "123" %)))
    (routes/app-routes)
    (re-frame/dispatch-sync [::events/initialize-db])
    (dev-setup)
    (mount-root)))
