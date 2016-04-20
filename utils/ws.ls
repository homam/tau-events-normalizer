{Obj, average, concat-map, drop, each, filter, find, fold, foldr1, gcd, id, keys, map, maximum, 
minimum, obj-to-pairs, sort, sum, tail, take, unique, mod, round, sort-by, group-by, floor, ceiling, mean, sqrt} = require \prelude-ls


Rx = require \rx
io = require \socket.io-client

from-web-socket = (address, open-observer) ->
    
    socket = io do 
        address
        reconnection: true
        force-new: true
        
    # Rx.Observable.create :: Observer -> (a -> Void)
    # observable :: Observable
    observable = Rx.Observable.create (observer) ->
        
        if !!open-observer
            socket.on \connect, ->
                open-observer.on-next!
                open-observer.on-completed!
            
        socket.io.on \packet, ({data}?) ->
            if !!data 
                observer.on-next do
                    name: data.0
                    data: data.1
        
        socket.on \error, (err) ->
            observer.on-error err

        socket.on \reconnect_error, (err) ->
            observer.on-error err

        socket.on \reconnect_failed, ->
            observer.on-error new Error 'reconnection failed'

        socket.io.on \close, ->
            observer.on-completed!

        !->
            socket.close!
            socket.destroy!

    observer = Rx.Observer.create (event) ->
        if !!socket.connected
            socket.emit event.name, event.data

    Rx.Subject.create observer, observable


module.exports = from-web-socket