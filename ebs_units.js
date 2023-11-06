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
    const coll = client.db('mdharura').collection('units');

    const docs = [];
    coll.aggregate([], async (cmdErr, result) => {

      let i = 1;

      await result.forEach((doc) => {
        console.log(i);
        console.log(JSON.parse(JSON.stringify(doc)));

        docs.push(JSON.parse(JSON.stringify(doc)));
        
        i++;
      });

      const data = await jsonexport(docs);

      fs.writeFile('./ebs_units.csv', data, (err) => {
        if (err) console.log(err);
        else console.log('Completed');
      });

      // const csv = new ObjectsToCsv(docs);

      // await csv.toDisk('./ebs_units.csv');

      
    });
  });