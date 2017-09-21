#!/usr/bin/env node

const WebSocket = require('ws');
const port = parseInt('bat', 36); // 14645, as in batman (the movie franchise)

const response = require('./response.js')

const wss = new WebSocket.Server({ port });
console.log("franchise-client listening on port", port)

wss.on('connection', ws => {
	console.log('opened connection')
	
	const ctx = {}

	ws.on('message', async message => {
		console.log('received:', message);

		message = JSON.parse(message)
		const {id} = message

		const res = await response(message, ctx)
		
		ws.send(JSON.stringify(Object.assign({id}, res)))

	})
})
