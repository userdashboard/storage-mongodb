const MongoDB = require('mongodb')

module.exports = {
  setup: async (moduleName) => {
    const mongodbURL = process.env[`${moduleName}_MONGODB_URL`] || process.env.MONGODB_URL
    let objectsCollection
    let db
    MongoDB.MongoClient.connect(mongodbURL, (error, mongoClient) => {
      if (error) {
        throw error
      }
      const client = module.exports.client = mongoClient
      const database = process.env[`${moduleName}_MONGODB_DATABASE`] || process.env.MONGODB_DATABASE || 'dashboard'
      db = client.db(database)
    })
    async function getCollection () {
      if (objectsCollection) {
        return objectsCollection
      }
      objectsCollection = await db.createCollection('objects')
      const indexes = await objectsCollection.indexes()
      if (indexes && indexes.length) {
        return objectsCollection
      }
      await objectsCollection.createIndex({ file: 1 }, { unique: true })
      return objectsCollection
    }
    const container = {
      exists: async (file) => {
        if (!file) {
          throw new Error('invalid-file')
        }
        const collection = await getCollection()
        const result = await collection.find({ file }, { _id: 1 }).limit(1).toArray()
        return result.length === 1
      },
      read: async (file) => {
        if (!file) {
          throw new Error('invalid-file')
        }
        const collection = await getCollection()
        const result = await collection.findOne({ file }, { contents: 1 })
        return result.contents
      },
      readMany: async (prefix, files) => {
        if (!files || !files.length) {
          throw new Error('invalid-files')
        }
        const appended = []
        for (const i in files) {
          appended[i] = `${prefix}/${files[i]}`
        }
        const collection = await getCollection()
        const data = {}
        const array = await collection.find({ file: { $in: appended } }).toArray()
        for (const i in files) {
          const itemid = files[i]
          const key = appended[i]
          for (const object of array) {
            for (const field in object) {
              if (field === key || object[field] === key) {
                data[itemid] = object.contents
                break
              }
              if (object[field] === itemid) {
                data[itemid] = object.contents
                break
              }
            }
          }
        }
        return data
      },
      readBinary: async (file) => {
        if (!file) {
          throw new Error('invalid-file')
        }
        const collection = await getCollection()
        const result = await collection.findOne({ file }, { buffer: 1 })
        return result.buffer
      },
      write: async (file, contents) => {
        if (!file) {
          throw new Error('invalid-file')
        }
        if (!contents && contents !== '') {
          throw new Error('invalid-contents')
        }
        const collection = await getCollection()
        const result = await collection.find({ file }, { _id: 1 }).limit(1).toArray()
        const existing = result.length === 1
        if (existing) {
          await collection.updateOne({ file }, { $set: { contents } }, { writeConcern: 1 })
          return
        }
        return collection.insertOne({ file, contents, created: new Date() }, { writeConcern: 1 })
      },
      writeBinary: async (file, buffer) => {
        if (!file) {
          throw new Error('invalid-file')
        }
        if (!buffer || !buffer.length) {
          throw new Error('invalid-buffer')
        }
        const collection = await getCollection()
        return collection.insertOne({ file, buffer, created: new Date().getTime() }, { writeConcern: 1 })
      },
      delete: async (file) => {
        if (!file) {
          throw new Error('invalid-file')
        }
        const collection = await getCollection()
        return collection.deleteOne({ file })
      }
    }
    if (process.env.NODE_ENV === 'testing') {
      const util = require('util')
      container.flush = util.promisify((callback) => {
        async function doFlush () {
          const collections = await db.collections()
          for (const collection of collections) {
            await collection.deleteMany({})
          }
          return callback()
        }
        if (!db) {
          return setTimeout(doFlush, 1)
        }
        return doFlush()
      })
    }
    return container
  }
}
