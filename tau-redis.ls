{first, map, foldl, each, empty, take} = require \prelude-ls
{get, set} = require \./utils/getset
{new-promise, from-callback, bind-p, return-p} = require \./utils/promise
{cache, tau} = (require \./redis/index)!
Rx = require \rx

now = Date.now
session = 30 * 60 * 1000
_ids = <[campaignVisitId sessionId clientSessionId visitId subscriberId campaignId ip ipTokens.ip2 ipTokens.ip3 platform creative suffix pageId country headers.user-agent uaTokens.os uaTokens.model originalQueryTokens]>


emitter = do ->
    events = new require('events')
    return new events.EventEmitter!


merge = ({q42, events}:me) ->
  
  q42-ids = events |> foldl do 
    (acc, a) ->
       _ids |> each (_id) ->
         set acc, _id, (get a, _id) # acc[_id] ?= a[_id]
       acc
    {}
  events: events |> map (e) -> {} <<< e <<< {q42-ids}
  ecount: events.length
  q42: q42


module.exports = start = (connect) ->

  connect!
    .flatMap (x) -> on-event x
    .subscribe do
        (x) ->
        (ex) ->
            console.error ex
        ->
            console.log "disconnected!"
            connect!

  on-event = ({q42}:event) ->

    o <- bind-p cache.get q42

    if !o
      o = {events: [], q42: q42, expiry: now! + session}
    
    exists = o.events.length > 0

    o.events.push event

    _ <- bind-p (cache.set q42, o)

    if !exists
      _ <- bind-p tau.push {q42, o.expiry}
      return-p event
    else
      return-p event


  schedule = (t = 1000) ->
    set-timeout do
      ->
        tau-processor!
          .catch (ex) -> console.error ex
      t

  tau-processor = ->


    o <- bind-p tau.shift!

    if !o
      emitter.emit 'info' {level: 1, message: 'tau is empty'}
      schedule!
    else
      {q42, expiry} = o
      delta = expiry - now!
      if delta > 0
         emitter.emit 'info' {level: 2, message: "oldest not expired, delta: #{delta}"}
         _ <- bind-p tau.back o
         schedule!
      else
         
        oldest <- bind-p cache.get q42
        if !oldest
          emitter.emit 'info' {level: 3, message: "oldest not found, probably already expired, q42: #{q42}, expiry: #{expiry}"}
          setImmediate tau-processor
        else
          emitter.emit 'q42-session-ended', (merge oldest)
          _ <- bind-p cache.remove q42
          setImmediate tau-processor


  tau-processor!
    .catch (ex) -> console.error ex



  output = do ->
      Rx.Observable.fromEventPattern do
        (h) -> emitter.addListener 'q42-session-ended', h
        (h) -> emitter.removeListener 'q42-session-ended', h

  info = do ->
      Rx.Observable.fromEventPattern do
        (h) -> emitter.addListener 'info', h
        (h) -> emitter.removeListener 'info', h
  {
    output
    info
  }

