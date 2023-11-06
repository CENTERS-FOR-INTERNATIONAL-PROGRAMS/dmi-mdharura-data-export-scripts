const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const {
  ObjectId
} = require('mongodb');
const jsonexport = require('jsonexport');
const fs = require('fs');

MongoClient.connect(
  'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false',
  { useNewUrlParser: true, useUnifiedTopology: true },
  function(connectErr, client) {
    assert.equal(null, connectErr);
    const coll = client.db('mdharura').collection('tasks');

    const docs = [];
    coll.aggregate([
      {
        '$lookup': {
          'from': 'users', 
          'localField': 'user', 
          'foreignField': '_id', 
          'as': 'user'
        }
      }, {
        '$unwind': {
          'path': '$user'
        }
      }, {
        '$lookup': {
          'from': 'units', 
          'localField': 'unit', 
          'foreignField': '_id', 
          'as': 'unit'
        }
      }, {
        '$unwind': {
          'path': '$unit'
        }
      }, {
        '$lookup': {
          'from': 'units', 
          'localField': 'unit.parent', 
          'foreignField': '_id', 
          'as': 'unit.parent'
        }
      }, {
        '$unwind': {
          'path': '$unit.parent'
        }
      }, {
        '$lookup': {
          'from': 'units', 
          'localField': 'unit.parent.parent', 
          'foreignField': '_id', 
          'as': 'unit.parent.parent'
        }
      }, {
        '$unwind': {
          'path': '$unit.parent.parent'
        }
      }
    ], async (cmdErr, result) => {

      let i = 1;

      await result.forEach((doc) => {
        console.log(i);
        console.log(JSON.parse(JSON.stringify(doc)));

        docs.push(JSON.parse(JSON.stringify(doc)));
        
        i++;
      });

      const data = await jsonexport(docs, { mapHeaders: (header) => header.replace(/\./g,'_').toUpperCase() });

      fs.writeFile('./ebs_tasks_1.csv', data, (err) => {
        if (err) console.log(err);
        else console.log('Completed');
      });

      // const csv = new ObjectsToCsv(docs);

      // await csv.toDisk('./ebs_tasks.csv');

      
    });
  });