const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const {
  ObjectId
} = require('mongodb');
const ObjectsToCsv = require('objects-to-csv')

const agg = [
  {
    '$match': {
      'facility': new ObjectId('5d68ad266369f84411d5608f')
    }
  }, {
    '$project': {
      '_id': 1, 
      'user': 1
    }
  }, {
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
    '$project': {
      '_id': '$user._id', 
      'user': '$user'
    }
  }, {
    '$lookup': {
      'from': 'invoices', 
      'localField': '_id', 
      'foreignField': 'user', 
      'as': 'invoice'
    }
  }, {
    '$unwind': {
      'path': '$invoice',
      'preserveNullAndEmptyArrays': true
    }
  }, {
    '$project': {
      '_id': '$_id', 
      'user': '$user', 
      'bill': '$invoice.total', 
      'status': '$invoice.status'
    }
  }, {
    '$group': {
      '_id': '$user', 
      'pending': {
        '$sum': {
          '$cond': [
            {
              '$eq': [
                '$status', 'PENDING'
              ]
            }, '$bill', 0
          ]
        }
      }, 
      'paid': {
        '$sum': {
          '$cond': [
            {
              '$eq': [
                '$status', 'COMPLETED'
              ]
            }, '$bill', 0
          ]
        }
      }, 
      'total': {
        '$sum': '$bill'
      }
    }
  }, {
    '$project': {
      '_id': 0, 
      'Name': '$_id.fullName', 
      'Phone': '$_id.phone',
      'Gender': '$_id.gender', 
      'Paid': '$paid',
      'Pending': '$pending',
      'Total': '$total',
    }
  }
];

MongoClient.connect(
  'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false',
  { useNewUrlParser: true, useUnifiedTopology: true },
  function(connectErr, client) {
    assert.equal(null, connectErr);
    const coll = client.db('uzima').collection('facilityroles');

    const docs = [];
    coll.aggregate(agg, async (cmdErr, result) => {

      let i = 1;

      await result.forEach((doc) => {
        console.log(i);
        console.log(doc);

        docs.push(doc);
        
        i++;
      });

      const csv = new ObjectsToCsv(docs);

      await csv.toDisk('./list.csv');

      console.log('Completed')
    });
  });