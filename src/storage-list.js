const indexedCollections = {}
let db

const MongoClient = require('mongodb').MongoClient
MongoClient.connect(process.env.MONGODB_URL, (error, mongoClient) => {
  if (error) {
    if (process.env.DEBUG_ERRORS) {
      console.log('[mongodb-storage]', error)
    }
    throw new Error('unknown-error')
  }
  const client = module.exports.client = mongoClient
  db = client.db(process.env.MONGODB_DATABASE || 'dashboard')
})

module.exports = {
  indexedCollections,
  add,
  count,
  exists,
  list,
  listAll,
  remove
}

if (process.env.NODE_ENV === 'testing') {
  module.exports.flush = async () => {
    for (const key in indexedCollections) {
      delete (indexedCollections[key])
    }
  }
}

async function exists (path, itemid) {
  const collection = await getCollection(path)
  const result = await collection.find({ path, itemid }, { _id: 1 }).limit(1).toArray()
  return result.length === 1
}

async function add (path, itemid) {
  const existing = await exists(path, itemid)
  if (existing) {
    return
  }
  const collection = await getCollection(path)
  return collection.insertOne({ path, itemid, created: new Date().getTime() }, { writeConcern: 1 })
}

async function count (path) {
  const collection = await getCollection(path)
  return collection.countDocuments({ path })
}

async function listAll (path) {
  const collection = await getCollection(path)
  const cursor = await collection.find({ path }).sort({ created: -1 })
  const items = await cursor.toArray()
  if (!items || !items.length) {
    return null
  }
  const results = []
  for (const item of items) {
    results.push(item.itemid)
  }
  return results
}

async function list (path, offset, pageSize) {
  offset = offset || 0
  if (pageSize === null || pageSize === undefined) {
    pageSize = global.pageSize
  }
  if (offset < 0) {
    throw new Error('invalid-offset')
  }
  const collection = await getCollection(path)
  const cursor = await collection.find({ path }).limit(pageSize).skip(offset)
  const items = await cursor.sort({ created: -1 }).toArray()
  if (!items || !items.length) {
    return null
  }
  const results = []
  for (const item of items) {
    results.push(item.itemid)
  }
  return results
}

async function remove (path, itemid) {
  const collection = await getCollection(path)
  return collection.deleteOne({ path, itemid })
}

async function getCollection (file) {
  let name = 'lists'
  if (file.indexOf('_') === -1) {
    name += '-' + file.split('/').join('-')
  }
  if (indexedCollections[name]) {
    return indexedCollections[name]
  }
  const collection = indexedCollections[name] = await db.createCollection(name)
  const indexes = await collection.indexes()
  if (indexes && indexes.length) {
    return collection
  }
  await collection.createIndex({ path: 1 }, { collation: { backwards: true, locale: 'en' } })
  await collection.createIndex({ created: -1 }, { collation: { backwards: true, locale: 'en' } })
  return collection
}
