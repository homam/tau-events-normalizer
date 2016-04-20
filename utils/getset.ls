{empty} = require \prelude-ls

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


module.exports = {
    get
    set
}