{Client, Producer, KeyedMessage}:kafka = require 'kafka-node'
client = new Client "ubuntu42:2181/", "homam"
producer = new Producer client

producer.on 'error', (ex) ->
	console.error ex

producer.on 'ready', ->
	console.log 'producer is ready'
	producer.send [{ topic: 'TutorialTopic', messages: 'how are you', partition: 0 }], (ex, data) ->
		console.error ex if !!ex
		console.log data