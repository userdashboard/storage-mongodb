let db, objectsCollection

const MongoClient = require('mongodb').MongoClient
MongoClient.connect(process.env.MONGODB_URL, (error, mongoClient) => {
  if (error) {
    if (process.env.DEBUG_ERRORS) {
      console.log('[mongodb-storage]', error)
    }
    throw new Error('unknown-error')
  }
  const client = mongoClient
  db = client.db(process.env.MONGODB_DATABASE || 'dashboard')
})

module.exports = {
  exists,
  read,
  readMany,
  readImage,
  write,
  writeImage,
  deleteFile
}

if (process.env.NODE_ENV === 'testing') {
  module.exports.flush = async () => {
    const collections = await db.collections()
    for (const collection of collections) {
      await collection.deleteMany({})
    }
  }
}

async function exists (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const collection = await getCollection()
  const result = await collection.find({ file }, { _id: 1 }).limit(1).toArray()
  return result.length === 1
}

async function deleteFile (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const collection = await getCollection()
  return collection.deleteOne({ file })
}

async function write (file, contents) {
  if (!file) {
    throw new Error('invalid-file')
  }
  if (!contents && contents !== '') {
    throw new Error('invalid-contents')
  }
  const collection = await getCollection()
  const existing = await exists(file)
  if (existing) {
    await collection.updateOne({ file }, { $set: { contents } }, { writeConcern: 1 })
    return
  }
  return collection.insertOne({ file, contents, created: new Date() }, { writeConcern: 1 })
}

async function writeImage (file, buffer) {
  if (!file) {
    throw new Error('invalid-file')
  }

  if (!buffer || !buffer.length) {
    throw new Error('invalid-buffer')
  }
  const collection = await getCollection()
  return collection.insertOne({ file, buffer, created: new Date().getTime() }, { writeConcern: 1 })
}

async function read (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const collection = await getCollection()
  const result = await collection.findOne({ file }, { contents: 1 })
  return result.contents
}

async function readMany (prefix, files) {
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
}

async function readImage (file) {
  if (!file) {
    throw new Error('invalid-file')
  }
  const collection = await getCollection()
  const result = await collection.findOne({ file }, { buffer: 1 })
  return result.buffer
}

async function getCollection () {
  if (objectsCollection) {
    return objectsCollection
  }
  objectsCollection = await db.collection('objects')
  const indexes = await objectsCollection.indexes()
  if (indexes && indexes.length) {
    return objectsCollection
  }
  await objectsCollection.createIndex({ file: 1 }, { unique: true })
  return objectsCollection
}
