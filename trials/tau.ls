{first, map, foldl, each} = require \prelude-ls
Rx = require \rx
RxNode = require \rx-node
from-web-socket = require \./ws
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
session = 10 * 60 * 1000
_ids = <[campaignVisitId sessionId clientSessionId visitId subscriberId]>

on-event = ({q42}:event) ->
  
  time = now!
  expiry = time + session
  absolute = time + session * 2

  o = cache[q42] ?= {refs: 0, events: [], q42: q42, absolute: absolute, deleted: false}
  o.events.push event

  o.expiry = expiry
  o.refs += 1
  tau.push o


schedule = (t = 1000) ->
  set-timeout tau-processor, t

expired = (o) ->
  return false if !o
  time = now!
  (o.absolute <= time) or (o.refs == 1 and o.expiry <= time)

tau-processor = ->
  #console.log \tau-processor, tau.length
  if tau.length == 0 
     schedule!   
  else
     oldest = first tau
     if oldest.deleted
        tau.shift!
        oldest.refs -= 1
        emitter.emit 'oldest-already-deleted', {oldest, length: tau.length}
        setImmediate tau-processor
     else

       if expired oldest
          # not expired yet
          if oldest.refs > 1
            tau.shift!
            oldest.refs -= 1
          else
            console.log \out
          schedule!
       else
          # expired
          tau.shift!
          oldest.refs -= 1
          if oldest.refs > 1
             setImmediate tau-processor
          else
             delete cache[oldest.q42]
             emitter.emit 'q42-session-ended', (merge {oldest.q42, oldest.events})
             oldest.deleted = true
             oldest.events = []
             setImmediate tau-processor


tau-processor!

merge = ({q42, events}:me) ->
  
  q42-ids = events |> foldl do 
    (acc, a) ->
       _ids |> each (_id) ->
         acc[_id] ?= a[_id]
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



