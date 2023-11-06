const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const ExcelJS = require('exceljs');

MongoClient.connect(
  'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false',
  { useNewUrlParser: true, useUnifiedTopology: true },
  function(connectErr, client) {
    assert.equal(null, connectErr);
    const coll = client.db('mdharura').collection('tasks');

    const docs = [];

    coll.aggregate([
      { 
        $match: { 
          state: 'live',
          $or: [
            { 'cebs.investigationForm': {
              $exists: true,
            } },
            { 'hebs.investigationForm': {
              $exists: true,
            } },
            { 'vebs.investigationForm': {
              $exists: true,
            } },
            {'lebs.investigationForm': {
              $exists: true,
            } },
          ]
        } 
      }, {
        $project: {
          _id: 0,
          unit: 1,
          date: {
            $switch: {
              branches: [
                {
                  case: { '$gt': ['$cebs.investigationForm', 0] },
                  then: '$cebs.investigationForm.createdAt'
                },
                {
                  case: { '$gt': ['$hebs.investigationForm', 0] },
                  then: '$hebs.investigationForm.createdAt'
                },
                {
                  case: { '$gt': ['$vebs.investigationForm', 0] },
                  then: '$vebs.investigationForm.createdAt'
                },
                {
                  case: { '$gt': ['$lebs.investigationForm', 0] },
                  then: '$lebs.investigationForm.createdAt'
                },
              ],
              default: null,
            },
          },
          user: {
            $switch: {
              branches: [
                {
                  case: { '$gt': ['$cebs.investigationForm', 0] },
                  then: '$cebs.investigationForm.user'
                },
                {
                  case: { '$gt': ['$hebs.investigationForm', 0] },
                  then: '$hebs.investigationForm.user'
                },
                {
                  case: { '$gt': ['$vebs.investigationForm', 0] },
                  then: '$vebs.investigationForm.user'
                },
                {
                  case: { '$gt': ['$lebs.investigationForm', 0] },
                  then: '$lebs.investigationForm.user'
                },
              ],
              default: null,
            },
          },
        },
      }, {
        $lookup: {
            from: 'users',
            localField: 'user',
            foreignField: '_id',
            as: 'user'
        }
      }, {
        $unwind: {
            path: '$user'
        }
      }, {
        $project: {
          name: '$user.displayName',
          mobile: '$user.phoneNumber',
          month : { $month : "$date" }, 
          year : { $year :  "$date" },
          unit: 1,
        },
      }, {
        $project: {
          name: 1,
          mobile: 1,
          month: 1,
          monthText : {
            $let: {
              vars: {
                monthsInString: [,'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
              },
              in: {
                $arrayElemAt: ['$$monthsInString', '$month']
              }
            }
          }, 
          year : 1,
          unit: 1,
        },
      }, {
        $group: {
          _id: {
            mobile: '$mobile',
            month: '$month',
            year: '$year',
          },
          sum: { $sum: 1 },
          month: { $first: '$monthText' },
          year: { $first: '$year' },
          unit: { $first: '$unit' },
          name: { $first: '$name' },
          mobile: { $first: '$mobile' },
        }
      }, {
        $sort: {
          '_id.year': -1,
          '_id.month': -1,
          'name': 1,
        }
      },
      {
        $lookup: {
            from: 'units',
            localField: 'unit',
            foreignField: '_id',
            as: 'level0'
        }
      }, {
          $unwind: {
              path: '$level0'
          }
      }, {
          $lookup: {
              from: 'units',
              localField: 'level0.parent',
              foreignField: '_id',
              as: 'level1'
          }
      }, {
          $unwind: {
              path: '$level1'
          }
      }, {
          $lookup: {
              from: 'units',
              localField: 'level1.parent',
              foreignField: '_id',
              as: 'level2'
          }
      }, {
          $unwind: {
              path: '$level2'
          }
      },
    ], async (cmdErr, result) => {

      await result.forEach((doc) => {

        var _doc = JSON.parse(JSON.stringify(doc));

        _doc['LevelId'] = _doc.unit;
        
        _doc['level'] = _doc.level0.name

        _doc['levelType'] = _doc.level0.type;

        if (_doc.level0.type=== 'Subcounty') {
          _doc[_doc.level0.type] = _doc.level0.name;
        }

        delete _doc.level0;

        _doc[_doc.level1.type] = _doc.level1.name;

        delete _doc.level1;

        if (_doc.level2.type !== 'Country') {
          _doc[_doc.level2.type] = _doc.level2.name;

          delete _doc.level2;
        }

        delete _doc.date;

        delete _doc.LevelId;

        delete _doc.unit;

        delete _doc._id;

        docs.push(_doc);
      });

      const workbook = new ExcelJS.Workbook();

      workbook.creator = 'm-Dharura Event Based Surveillance System';
      workbook.created = new Date();
      workbook.company = 'Washington State University - Global Health Kenya'

      docs.forEach(doc => {
        var worksheet = workbook.getWorksheet(`${doc.month} ${doc.year}`);

        if (!worksheet) {
          worksheet = workbook.addWorksheet(`${doc.month} ${doc.year}`, {});

          worksheet.addRow(['Name', 'Mobile', 'Risk Assessments', 'Compensation (KES)', 'Subcounty', 'County']);
        }

        worksheet.addRow([doc.name, doc.mobile, doc.sum, doc.sum * 30, doc.Subcounty, doc.County]);
      });

      await workbook.xlsx.writeFile('./mdharura.risk.assessment.compensation.xlsx');

      process.exit(0);

    });
  });