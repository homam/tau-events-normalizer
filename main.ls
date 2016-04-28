Rx = require \rx
RxNode = require \rx-node
from-web-socket = require \./utils/ws
semaphore = (require './utils/semaphore') 600

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


record = require \./utils/esls

{output, info, listen} = (require \./tau-redis) semaphore, (({q42, events}) -> record {creationTime: Date.now!, events})
listen connect

trace = (msg, x) ->
    console.log msg
    x


output = output
    .map ({q42, events}:me) ->
        (JSON.stringify me) + "\n"

info
    .filter (.level > -1)
    .subscribe (x) ->
        console.log x


fs = require \fs
writer = fs.createWriteStream './output.bson', {flags: 'a'}

subscription = RxNode.writeToStream output, writer, 'utf8'

