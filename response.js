const { Client: PostgresClient } = require('pg')
const mysql = require('mysql2/promise')
const BigQueryClient = require('@google-cloud/bigquery')
const tmp = require('tmp')
const fs = require('fs')

const credentials = require('./credentials.js')


const localCtx = {}
module.exports = async function response(message, ctx=localCtx){
    const {action, id} = message

    try {

        if(action === 'open') {
            const {credentials, db} = message

            ctx.client = await createClient(db, credentials)
            return {ready: true}

        } else if(action === 'exec') {
            const {sql} = message

            const results = await ctx.client.query(sql, message)
            return {results}

        } else if(action === 'close') {
            await ctx.client.close()

            return {closed: true}

        } else if(action == 'get_postgres_credentials') {

            return credentials

        } else if(action == 'get_bigquery_schema') {

            const get = async (o, prop, ...rest) => 
                typeof prop === 'undefined'     ? o
                : typeof o[prop] === 'function' ? get(await o[prop](), ...rest)
                : Array.isArray(o[prop])        ? Promise.all(o[prop].map(sub => get(sub, ...rest)))
                : typeof prop === 'function' ? get(await prop(o), ...rest)
                : new Error('not found: ' + o + ' ' + prop)

            const flatten = (arr, result = []) => {
                arr.forEach(value => Array.isArray(value) ? flatten(value, result) : result.push(value))
                return result
            }

            const raw = await get(
                ctx.client,
                'getDatasets',
                0,
                'getTables',
                0,
                'getMetadata',
                metadata => metadata[0])

            const schema = flatten(raw).map(table => ({
                schema: table.tableReference.datasetId,
                name: table.tableReference.tableId,
                columns: table.schema.fields.map(f => f.name)
            }))

            return {schema}

        } else {
            throw new Error('Unknown action: '+action)
        }

    } catch(e) {
        console.log(e)
        return {error: e.message || e.stack.split('\n')[0]}
    }
}


async function createClient(db, credentials){
	if(db === 'postgres') return await createPostgresClient(credentials);
	if(db === 'bigquery') return await createBigQueryClient(credentials);
	if(db === 'mysql') return await createMySQLClient(credentials);
	throw new Error('database ' + db + ' not recognized')
}


async function createMySQLClient(credentials){
	const client = await mysql.createConnection(credentials)
	return {
		async query(sql){
			const [rows, fields] = await client.execute(sql);
			console.log(rows, fields)
			if(fields){
				const field_list = fields.map(k => k.name);
				return {
					columns: field_list,
					values: rows.map(row => field_list.map(k => row[k]))
				}	
			}else{
				return {
					columns: ['result'],
					values: [[ rows ]]
				}
			}
			
		},
		async close(){
			return await client.end()
		}
	}
}

async function createPostgresClient(credentials){
    const client = new PostgresClient(credentials)
    ;[1082,1114,1184].forEach(oid => client.setTypeParser(oid, val => val))
    await client.connect()
    return {
        async query(sql){
            let results = await client.query({
                text: sql,
                rowMode: 'array'
            })
            if(Array.isArray(results)){
                results = results[results.length - 1]
            }
            // console.log(results.rows, results)
            if(results.rows.length > 10000)
                throw new Error('Too many result rows to serialize: Try using a LIMIT statement.')
            return results
        },
        close: client.end.bind(client)
    }
}

function createBigQueryClient(credentials){
    if(credentials.keyFile){
        const {name, data} = credentials.keyFile

        const {name: keyFilename, fd} = tmp.fileSync({postfix: name})
        fs.writeFileSync(fd, Buffer.from(data, 'hex'))

        credentials.keyFilename = keyFilename
    }
    console.log(credentials)
    const client = new BigQueryClient(credentials)
    return {
        query: (sql, {useLegacySql}) => client.query({query: sql, useLegacySql}),
        getDatasets: () => client.getDatasets(),
        close(){ console.log('no bigquery close method') }
    }
}
