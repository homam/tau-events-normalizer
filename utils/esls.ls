require! \moment
{fold, map, Str} = require \prelude-ls
require! \request

# :: 
new-promise = (res, rej) -> new Promise res, rej

# :: p a -> (a -> b) -> p b
bind-p = (p, f) -> p.then f

# :: String -> a -> p ()
post-content = (url, body) ->
    res, rej <- new-promise
    err, , result <- request.post {url, body}
    if err then rej err else res result

# :: String -> ElasticsearchClient
ElasticsearchClient = (endpoint) ->

    # :: String -> p Boolean
    does-index-exist = (index) ->
        resolve, reject <- new-promise
        err, response <- request.head "#{endpoint}/#{index}"
        if err
            reject err
        else
            resolve response.status-code == 200

    # :: String -> p ()
    create-index-if-not-exists = (index) ->
        resolve, reject <- new-promise
        exists <- bind-p does-index-exist index
        if exists
            resolve true
        else
            err <- request.put "#{endpoint}/#{index}"
            if err then reject err else resolve null

    # :: String -> String -> [a] -> p ()
    bulk-insert = (index, type, items) ->
        post-content do 
            "#{endpoint}/#{index}/#{type}/_bulk"
            items 
                |> fold do 
                    (acc, event) -> acc ++= [{create: {}}, event]
                    []
                |> map JSON.stringify
                |> Str.join '\n'
                |> -> "#{it}\n"

    # :: String -> String -> Int -> a -> p ()
    bulk-insert-to-time-based-index = do ->
        cache = {}
        (index, type, creation-time, items) -->
            moment-of-creation = moment creation-time
            full-index-name = "#{index}_#{moment-of-creation.year!}_#{moment-of-creation.week!}"

            # create index if not exists
            <- bind-p do ->
                if cache[full-index-name]
                    Promise.resolve cache[full-index-name]
                
                else
                    <- bind-p create-index-if-not-exists full-index-name
                    cache[full-index-name] = true

            bulk-insert full-index-name, type, items

    {bulk-insert-to-time-based-index}

elasticsearch-client = new ElasticsearchClient \http://146.20.12.209:9200

module.exports = insert-normalized-q42 = ({creation-time, events}) ->
    elasticsearch-client.bulk-insert-to-time-based-index do
        \mobi_one_normalized
        \event
        creation-time
        events
