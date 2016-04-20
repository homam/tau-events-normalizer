Redis = require \redis
{new-promise, from-callback, bind-p, return-p} = require \./../utils/promise
{map} = require \prelude-ls

module.exports = connect = ->

  client = Redis.createClient!
  client.on \error, (ex) ->
    console.error "Redis error:", ex

  cache = do ->
    [hget, hset, hdel] = map ((name) -> from-callback client[name], client), <[hget hset hdel]>
    get: (key) ->
      (hget "q42", key).then (x) -> JSON.parse x
    set: (key, value) ->
      hset "q42", key, (JSON.stringify value)
    remove: (key) ->
      hdel "q42", key

  tau = do -> 
    [rpush, lpush, lpop] = map ((name) -> from-callback client[name], client), <[rpush lpush lpop]>
    push: (o) ->
      rpush "q42tau", (JSON.stringify o)
    back: (o) ->
      lpush "q42tau", (JSON.stringify o)
    shift: ->
      (lpop "q42tau").then (x) -> JSON.parse x

  {
    cache
    tau
  }