r = require \rethinkdb
Rx = require \rx
from-web-socket = require \./ws
bind-p = (p, f) --> p.then f

con <- bind-p r.connect {host: '192.168.99.100', port: 32769, db: 'test'}

ws = from-web-socket 'ws://104.130.161.64:4040'
    .filter ({name}) -> name == \all-campaigns
    .map ({data}) -> JSON.parse data
    .map (x) ->  Rx.Observable.from-promise (r.table \events .insert x .run con)
    .map -> 1
    .scan (+), 0
    .subscribe do
        (x) ->
            console.log x
            #ws.dispose!
        (ex) ->
            console.error ex
        ->
            console.log \completed




use-conn = (conn, f) --> new Promise (resolve, reject) ->
    f conn
        .then (x) ->
            console.log "conn.close then"
            conn.close!
            .then ->
                resolve {result: x}
            .catch (ex) ->
                reject {result: x, closingException: ex}

        .catch (ex) ->
            console.log "conn.close catch"
            conn.close!
            .then ->
                reject {exception: ex}
            .catch (exx) ->
                reject {exception: ex, closingException: exx}
    
use = (f) ->
    conn <- bind-p r.connect {host: '192.168.99.100', port: 32769, db: 'test'}
    use-conn conn, f

# go = -> use (con) ->
#     # r.db \test .tableList!.run conn
#     #r.dbList!.run conn
#     #r.tableList!.run con

#     r.table \duck .insert {
#         name: 'hduck'
#     } .run con

# go!
#     .then (x) ->
#         console.log x
#     .catch (ex) ->
#         console.log "exception", ex