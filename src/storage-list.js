const Log = require('@userdashboard/src/log.js')('storage-mongodb')
const MongoDB = require('mongodb')

module.exports = {
  setup: async (storage, moduleName) => {
    const mongodbURL = process.env[`${moduleName}_MONGODB_URL`] || process.env.MONGODB_URL
    const indexedCollections = {}
    let db
    MongoDB.MongoClient.connect(mongodbURL, (error, mongoClient) => {
      if (error) {
        Log.error('mongodb storage error', error)
        throw new Error('unknown-error')
      }
      const client = module.exports.client = mongoClient
      db = client.db(process.env.MONGODB_DATABASE || 'dashboard')
    })
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
    const container = {
      add: async (path, itemid) => {
        const collection = await getCollection(path)
        const result = await collection.find({ path, itemid }, { _id: 1 }).limit(1).toArray()
        const existing = result.length === 1
        if (existing) {
          return
        }
        return collection.insertOne({ path, itemid, created: new Date().getTime() }, { writeConcern: 1 })
      },
      count: async (path) => {
        const collection = await getCollection(path)
        return collection.countDocuments({ path })
      },
      exists: async (path, itemid) => {
        const collection = await getCollection(path)
        const result = await collection.find({ path, itemid }, { _id: 1 }).limit(1).toArray()
        return result.length === 1
      },
      list: async (path, offset, pageSize) => {
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
      },
      listAll: async (path) => {
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
      },
      remove: async (path, itemid) => {
        const collection = await getCollection(path)
        return collection.deleteOne({ path, itemid })
      }
    }
    if (process.env.NODE_ENV === 'testing') {
      container.flush = async () => {
        for (const key in indexedCollections) {
          delete (indexedCollections[key])
        }
      }
    }
    return container
  }
}
