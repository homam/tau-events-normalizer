new-promise = (callback) ->
  new Promise (res, rej) ->
    callback res, rej

from-callback = (f, self = null) ->
  (...args) ->
    _res = null
    _rej = null
    args = args ++ [(error, result) ->
      return _rej error if !!error
      _res result
    ]
    (res, rej) <- new-promise
    _res := res
    _rej := rej
    try
      f.apply self, args
    catch ex
      rej ex

bind-p = (p, f) --> p.then f

return-p = (val) -> Promise.resolve val

module.exports = {
  new-promise
  from-callback
  bind-p
  return-p
}
