{first, map, foldl, each, empty} = require \prelude-ls
Rx = require \rx
RxNode = require \rx-node
from-web-socket = require \./utils/ws
bind-p = (p, f) --> p.then f

emitter = do ->
    events = new require('events')
    return new events.EventEmitter!


do ->
  connect = -> 
    from-web-socket 'ws://104.130.161.64:4040'
      .filter ({name}) -> name == \all-campaigns
      .map ({data}) -> JSON.parse data
      .filter (x) -> !!x.q42
      #.filter (x) -> x.ip == '80.227.47.62' && x.headers?['user-agent'] == 'HOMAM'
      .subscribe do
          (x) ->
              on-event x
              #ws.dispose!
          (ex) ->
              console.error ex
          ->
              console.log "disconnected!"
              connect!

  connect!



cache = {}
tau = []
now = Date.now
session = 30 * 60 * 1000
_ids = <[campaignVisitId sessionId clientSessionId visitId subscriberId campaignId ip ipTokens.ip2 ipTokens.ip3 platform creative suffix pageId country headers.user-agent uaTokens.os uaTokens.model originalQueryTokens]>

on-event = ({q42}:event) ->
  
  o = cache[q42] ?= {events: [], q42: q42, expiry: now! + session}
  exists = o.events.length > 0
  o.events.push event

  if !exists
    tau.push o


schedule = (t = 1000) ->
  set-timeout tau-processor, t

tau-processor = ->
  #console.log \tau-processor, tau.length
  if tau.length == 0 
     schedule!   
  else
     oldest = first tau
     delta = oldest.expiry - now!
     if delta > 0
        emitter.emit 'oldest-not-expired', {delta}
        schedule!
     else
        tau.shift!
        emitter.emit 'q42-session-ended', (merge oldest)
        delete cache[oldest.q42]
        setImmediate tau-processor


tau-processor!


get = (o, selector) -->
    _get = (o, selector) ->
        [h, ...t] = selector
        if empty t
            o[h]
        else
            _get o[h], t
            
    _get o, (selector.split \.)

set = (o, selector, value) -->
    _set = (o, selector) ->
        [h, ...t] = selector
        if empty t
            o[h] ?= value
        else
            _set (o[h] ? {}), t
            
    _set o, (selector.split \.)   

merge = ({q42, events}:me) ->
  
  q42-ids = events |> foldl do 
    (acc, a) ->
       _ids |> each (_id) ->
         set acc, _id, (get a, _id)
         # acc[_id] ?= a[_id]
       acc
    {}
  events: events |> map (e) -> {} <<< e <<< {q42-ids}
  ecount: events.length
  q42: q42


output = do ->
    Rx.Observable.fromEventPattern do
      (h) -> emitter.addListener 'q42-session-ended', h
      (h) -> emitter.removeListener 'q42-session-ended', h
    .map ({q42, events}:me) ->
          #console.log "session #{events.length}, #{q42}"
          (JSON.stringify me) + "\n"



fs = require \fs
writer = fs.createWriteStream './output.bson', {flags: 'a'}

subscription = RxNode.writeToStream output, writer, 'utf8'

# output
#     .subscribe do
#         ({q42, events}:me) ->
#           #console.log (JSON.stringify xs, null, 4)
#           console.log "session #{events.length}, #{q42}"
#           writer.write (JSON.stringify me) + "\n"
#         (ex) ->
#             console.error ex
#         ->
#             console.log \completed

save-db-all = (events) -> console.log events



