Rx = require \rx
RxNode = require \rx-node
from-web-socket = require \./utils/ws

connect = -> 
  from-web-socket 'ws://104.130.161.64:4040'
    .filter ({name}) -> name == \all-campaigns
    .map ({data}) -> JSON.parse data
    # .map (x) -> 
    #     if x.country != 'PK' and x.eventType == 'subscription' and !x.q42
    #         console.log 'subscription', x
    #     x
    .filter (x) -> !!x.q42
    #.filter (x) -> x.ip == '80.227.47.62' && x.headers?['user-agent'] == 'HOMAM'


insert = ({events}) ->
    record {creationTime: Date.now!, events}

{output, info, listen} = (require \./tau-redis) {insert}
listen connect
record = require \./utils/esls

trace = (msg, x) ->
    console.log msg
    x

output = output
    #.flatMapWithMaxConcurrent 100, ({events}) -> Rx.Observable.defer (-> record {creationTime: Date.now!, events})
    .map ({q42, events}:me) ->
        (JSON.stringify me) + "\n"

info
    #.filter (.level > 2)
    .subscribe (x) ->
        console.log x

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


