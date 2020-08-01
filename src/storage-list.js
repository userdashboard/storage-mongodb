const fs = require('fs')
const MongoDB = require('mongodb')
const path = require('path')
const util = require('util')

module.exports = {
  setup: util.promisify((storage, moduleName, callback) => {
    if (!callback && moduleName) {
      callback = moduleName
      moduleName = null
    }
    const mongodbURL = process.env[`${moduleName}_MONGODB_URL`] || process.env.MONGODB_URL
    const dashboardPath1 = path.join(global.applicationPath, 'node_modules/@userdashboard/dashboard/src/log.js')
    let Log
    if (fs.existsSync(dashboardPath1)) {
      Log = require(dashboardPath1)('postgresql-list')
    } else {
      const dashboardPath2 = path.join(global.applicationPath, 'src/log.js')
      Log = require(dashboardPath2)('postgresql-list')
    }
    const indexedCollections = {}
    let db
    return MongoDB.MongoClient.connect(mongodbURL, (error, mongoClient) => {
      if (error) {
        Log.error('error connecting', error)
        return callback(new Error('unknown-error'))
      }
      const client = module.exports.client = mongoClient
      db = client.db(process.env.MONGODB_DATABASE || 'dashboard')
      const container = {
        add: util.promisify((path, itemid, callback) => {
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.insertOne({ path, itemid, created: new Date().getTime() }, { writeConcern: 1 }, (error) => {
              if (error) {
                Log.error('error adding', error)
                return callback(new Error('unknown-error'))
              }
              return callback()
            })
          })
        }),
        addMany: util.promisify((items, callback) => {
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            const objects = []
            const created = new Date().getTime()
            for (const key in items) {
              objects.push({ path: key, itemid: items[key], created })
            }
            return collection.insertMany(objects, { writeConcern: 1 }, (error) => {
              if (error) {
                Log.error('error adding', error)
                return callback(new Error('unknown-error'))
              }
              return callback()
            })
          })
        }),
        count: util.promisify((path, callback) => {
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.countDocuments({ path }, (error, count) => {
              if (error) {
                Log.error('error adding', error)
                return callback(new Error('unknown-error'))
              }
              return callback(null, count)
            })
          })
        }),
        exists: util.promisify((path, itemid, callback) => {
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.find({ path, itemid }, { _id: 1 }).toArray((error, result) => {
              if (error) {
                Log.error('error checking exists', error)
                return callback(new Error('unknown-error'))
              }
              return callback(null, result.length === 1)
            })
          })
        }),
        list: util.promisify((path, offset, pageSize, callback) => {
          if (!callback) {
            if (pageSize) {
              callback = pageSize
              pageSize = null
            } else if (offset) {
              callback = offset
              offset = null
            }
          }
          offset = offset || 0
          if (pageSize === null || pageSize === undefined) {
            pageSize = global.pageSize
          }
          if (offset < 0) {
            return callback(new Error('invalid-offset'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.find({ path }).sort({ created: -1 }).skip(offset).limit(pageSize).toArray((error, results) => {
              if (error) {
                Log.error('error listing', error)
                return callback(new Error('unknown-error'))
              }
              if (!results || !results.length) {
                return callback()
              }
              const items = []
              for (const result of results) {
                items.push(result.itemid)
              }
              return callback(null, items)
            })
          })
        }),
        listAll: util.promisify((path, callback) => {
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.find({ path }).sort({ created: -1 }).toArray((error, results) => {
              if (error) {
                Log.error('error listing all', error)
                return callback(new Error('unknown-error'))
              }
              if (!results || !results.length) {
                return callback()
              }
              const items = []
              for (const result of results) {
                items.push(result.itemid)
              }
              return callback(null, items)
            })
          })
        }),
        remove: util.promisify((path, itemid, callback) => {
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.deleteOne({ path, itemid }, (error) => {
              if (error) {
                Log.error('error deleting', error)
                return callback(new Error('unknown-error'))
              }
              return callback()
            })
          })
        })
      }
      if (process.env.NODE_ENV === 'testing') {
        container.flush = util.promisify((callback) => {
          for (const key in indexedCollections) {
            delete (indexedCollections[key])
          }
          return callback()
        })
      }
      return callback(null, container)
    })

    function getCollection (callback) {
      if (indexedCollections.lists) {
        return callback(null, indexedCollections.lists)
      }
      return db.listCollections((error, collections) => {
        if (error) {
          Log.error('error listing collections', error)
          return callback(new Error('unknown-error'))
        }
        Log.info(collections)
        if (collections && collections.length) {
          for (const collection of collections) {
            if (collection.name === 'lists') {
              indexedCollections.lists = collection
              return callback(null, collection)
            }
          }
        }
        return db.createCollection('lists', (error, collection) => {
          if (error) {
            Log.error('error creating collection', error)
            return callback(new Error('unknown-error'))
          }
          indexedCollections.lists = collection
          return collection.indexes((error, indexes) => {
            if (error) {
              Log.error('error retrieving indexes', error)
              return callback(new Error('unknown-error'))
            }
            if (indexes && indexes.length) {
              return callback(null, collection)
            }
            return collection.createIndex({ path: 1 }, { collation: { backwards: true, locale: 'en' } }, (error) => {
              if (error) {
                Log.error('error creating index1', error)
                return callback(new Error('unknown-error'))
              }
              return collection.createIndex({ created: -1 }, { collation: { backwards: true, locale: 'en' } }, (error) => {
                if (error) {
                  Log.error('error retrieving index2', error)
                  return callback(new Error('unknown-error'))
                }
                return callback(null, collection)
              })
            })
          })
        })
      })
    }
  })
}
