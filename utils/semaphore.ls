Semaphore = (capacity) ->
  capacity++
  callbacks = []
  size = 0
  sem = {
    getSize: -> size
    getCallbacks: -> callbacks
    waitOne: (cb) ->
      size++
      if size >= capacity
        callbacks.push cb
      else 
        cb!
    release: ->
      size--
      cb = callbacks.shift!
      if !!cb
        cb!
  }
  sem

module.exports = Semaphore