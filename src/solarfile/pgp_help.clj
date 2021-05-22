(ns solarfile.pgp-help
  (:require [clj-pgp.core :as pgp]
            [clj-pgp.keyring :as keyring]
            [clj-pgp.message :as pgp-msg]))

(comment

  (def keyring (keyring/load-secret-keyring (slurp "resources/keys/privkey.asc")))
  (def pubkey (first (keyring/list-public-keys keyring)))
  (pgp/key-id pubkey)

  "or"

  (def pubkey (pgp/decode-public-key (slurp "resources/keys/pubkey.asc")))

  (def message (pgp-msg/encrypt "Hello World" pubkey
                                :format :utf8 :cipher :aes-256 :compress :zip :armor true))

  (def seckey (keyring/get-secret-key keyring (pgp/hex-id pubkey)))

  (println message)
  (pgp/key-algorithm seckey)
  (pgp/key-info pubkey)
  (pgp/key-info seckey)

  (def privkey (pgp/unlock-key seckey "welcome"))

  (pgp-msg/decrypt message privkey)

  (spit "resources/encrypt.txt.pgp" message))