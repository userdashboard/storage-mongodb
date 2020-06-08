const fs = require('fs')
const MongoDB = require('mongodb')
const path = require('path')
const util = require('util')

module.exports = {
  setup: util.promisify((moduleName, callback) => {
    if (!callback) {
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
        return callback(error)
      }
      const client = module.exports.client = mongoClient
      const database = process.env[`${moduleName}_MONGODB_DATABASE`] || process.env.MONGODB_DATABASE || 'dashboard'
      db = client.db(database)
      const container = {
        exists: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.find({ file }, { _id: 1, limit: 1 }, (error, cursor) => {
              if (error) {
                Log.error('error checking exists', error)
                return callback(new Error('unknown-error'))
              }
              return cursor.toArray((error, result) => {
                if (error) {
                  Log.error('error checking exists', error)
                  return callback(new Error('unknown-error'))
                }
                return callback(null, result.length === 1)
              })
            })
          })
        }),
        read: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.findOne({ file }, { contents: 1 }, (error, result) => {
              if (error) {
                Log.error('error reading', error)
                return callback(new Error('unknown-error'))
              }
              return callback(null, result.contents)
            })
          })
        }),
        readMany: util.promisify((prefix, files, callback) => {
          if (!files || !files.length) {
            return callback(new Error('invalid-files'))
          }
          const appended = []
          for (const i in files) {
            appended[i] = `${prefix}/${files[i]}`
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            const data = {}
            return collection.find({ file: { $in: appended } }, (error, cursor) => {
              if (error) {
                Log.error('error reading many', error)
                return callback(new Error('unknown-error'))
              }
              return cursor.toArray((error, result) => {
                if (error) {
                  Log.error('error reading many', error)
                  return callback(new Error('unknown-error'))
                }
                for (const i in files) {
                  const itemid = files[i]
                  const key = appended[i]
                  for (const object of result) {
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
                return callback(null, data)
              })
            })
          })
        }),
        readBinary: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.findOne({ file }, { buffer: 1 }, (error, result) => {
              if (error) {
                Log.error('error reading binary', error)
                return callback(new Error('unknown-error'))
              }
              return callback(null, result.buffer)
            })
          })
        }),
        write: util.promisify((file, contents, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          if (!contents && contents !== '') {
            return callback(new Error('invalid-contents'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.find({ file }, { _id: 1, limit: 1 }, (error, cursor) => {
              if (error) {
                Log.error('error writing', error)
                return callback(new Error('unknown-error'))
              }
              return cursor.toArray((error, result) => {
                if (error) {
                  Log.error('error writing', error)
                  return callback(new Error('unknown-error'))
                }
                const existing = result.length === 1
                if (existing) {
                  return collection.updateOne({ file }, { $set: { contents } }, (error) => {
                    if (error) {
                      Log.error('error writing', error)
                      return callback(new Error('unknown-error'))
                    }
                    return callback()
                  })
                }
                return collection.insertOne({ file, contents, created: new Date() }, { writeConcern: 1 }, (error) => {
                  if (error) {
                    Log.error('error writing', error)
                    return callback(new Error('unknown-error'))
                  }
                  return callback()
                })
              })
            })
          })
        }),
        writeBinary: util.promisify((file, buffer, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          if (!buffer || !buffer.length) {
            return callback(new Error('invalid-buffer'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.insertOne({ file, buffer, created: new Date().getTime() }, { writeConcern: 1 }, (error) => {
              if (error) {
                Log.error('error writing binary', error)
                return callback(new Error('unknown-error'))
              }
              return callback()
            })
          })
        }),
        delete: util.promisify((file, callback) => {
          if (!file) {
            return callback(new Error('invalid-file'))
          }
          return getCollection((error, collection) => {
            if (error) {
              return callback(error)
            }
            return collection.deleteOne({ file }, (error) => {
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
        const util = require('util')
        container.flush = util.promisify((callback) => {
          function doFlush () {
            if (!db) {
              return setTimeout(doFlush, 1)
            }
            return db.collections((error, collections) => {
              if (error) {
                Log.error('error flushing', error)
                return callback(new Error('unknown-error'))
              }
              function nextItem () {
                if (!collections.length) {
                  return callback()
                }
                const collection = collections.shift()
                return collection.deleteMany({}, (error) => {
                  if (error) {
                    Log.error('error flushing', error)
                    return callback(new Error('unknown-error'))
                  }
                  return nextItem()
                })
              }
              return nextItem()
            })
          }
          if (!db) {
            return setTimeout(doFlush, 1)
          }
          return doFlush()
        })
      }
      return callback(null, container)
    })

    function getCollection (callback) {
      if (indexedCollections.objects) {
        return callback(null, indexedCollections.objects)
      }
      return db.createCollection('objects', (error, collection) => {
        if (error) {
          Log.error('error creating collection', error)
          return callback(new Error('unknown-error'))
        }
        indexedCollections.objects = collection
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
    }
  })
}
