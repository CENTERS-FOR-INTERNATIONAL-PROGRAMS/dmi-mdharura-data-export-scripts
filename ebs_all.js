const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");
const { ObjectId } = require("mongodb");
const jsonexport = require("jsonexport");
const fs = require("fs");

const requiredUnits = [
  // ObjectId("60ac8ca55ff3fb154fbab8cf"), // Kenya
  ObjectId("60ac8ca65ff3fb154fbab932"), // Busia
  ObjectId("60ac8caa5ff3fb154fbabca7"), // Siaya
  ObjectId("60ac8ca65ff3fb154fbab9a0"), // Nakuru
  ObjectId("60ac8ca55ff3fb154fbab8d2"), // Meru
  ObjectId("60ac8ca95ff3fb154fbabbda"), // Nairobi
  ObjectId("60ac8ca75ff3fb154fbaba1d"), // Mombasa
];

const requiredCollections = [
  "tasks", // Tasks collections
  // "units", // Units collection
  // "roles", // Roles collection
  // "users", // Units collection
];

const createdAt = {
  $gte: new Date("2023-01-01T00:00:00.000Z"),
  $lt: new Date("2023-02-01T00:00:00.000Z"),
};

MongoClient.connect(
  "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false",
  { useNewUrlParser: true, useUnifiedTopology: true },
  function (connectErr, client) {
    assert.equal(null, connectErr);

    client.db("mdharura").collections({}, (error, collections) => {
      assert.equal(null, error);

      for (const collection of collections) {
        const query = [];

        switch (collection.namespace) {
          case "mdharura.roles":
            query.push({
              $match: {
                units: {
                  $in: requiredUnits,
                },
              },
            });
            break;
          case "mdharura.units":
            query.push({
              $match: {
                units: {
                  $in: requiredUnits,
                },
              },
            });
            break;
          case "mdharura.tasks":
            query.push(
              {
                $match: {
                  state: "live",
                  lebs: {
                    $exists: false,
                  },
                  units: {
                    $in: requiredUnits,
                  },
                  createdAt,
                },
              },
              {
                $lookup: {
                  from: "units",
                  localField: "unit",
                  foreignField: "_id",
                  as: "level0",
                },
              },
              {
                $unwind: {
                  path: "$level0",
                },
              },
              {
                $lookup: {
                  from: "units",
                  localField: "level0.parent",
                  foreignField: "_id",
                  as: "level1",
                },
              },
              {
                $unwind: {
                  path: "$level1",
                },
              },
              {
                $lookup: {
                  from: "units",
                  localField: "level1.parent",
                  foreignField: "_id",
                  as: "level2",
                },
              },
              {
                $unwind: {
                  path: "$level2",
                },
              }
            );
            break;
        }

        collection.aggregate(query, async (cmdErr, result) => {
          const docs = [];

          if (!requiredCollections.includes(collection.namespace.split(".")[1]))
            return;

          switch (collection.namespace) {
            case "mdharura.tasks": {
              docs.push({
                _id: "",
                createdAt: "",
                signal: "",
                signalId: "",
                state: "",
                status: "",
                LevelId: "",
                Level: "",
                "Level Type": "",
                Subcounty: "",
                County: "",
              });
            }
          }

          await result.forEach((doc) => {
            var _doc = JSON.parse(JSON.stringify(doc));

            switch (collection.namespace) {
              case "mdharura.tasks": {
                _doc["LevelId"] = _doc.unit;

                _doc["Level"] = _doc.level0.name;

                _doc["Level Type"] = _doc.level0.type;

                if (_doc.level0.type === "Subcounty") {
                  _doc[_doc.level0.type] = _doc.level0.name;
                }

                delete _doc.level0;

                _doc[_doc.level1.type] = _doc.level1.name;

                delete _doc.level1;

                if (_doc.level2.type !== "Country") {
                  _doc[_doc.level2.type] = _doc.level2.name;

                  delete _doc.level2;
                }

                try {
                  const {
                    verificationForm,
                    investigationForm,
                    responseForm,
                    escalationForm,
                  } = _doc.cebs || _doc.hebs || _doc.vebs;

                  if (verificationForm) {
                    delete verificationForm._id;
                    delete verificationForm.updatedAt;

                    _doc = { ..._doc, ...{ verificationForm } };
                  }

                  if (investigationForm) {
                    delete investigationForm._id;
                    delete investigationForm.updatedAt;

                    _doc = { ..._doc, ...{ investigationForm } };
                  }

                  if (responseForm) {
                    delete responseForm._id;
                    delete responseForm.updatedAt;

                    _doc = { ..._doc, ...{ responseForm } };
                  }

                  if (escalationForm) {
                    delete escalationForm._id;
                    delete escalationForm.updatedAt;
                    _doc = { ..._doc, ...{ escalationForm } };
                  }
                } catch (e) {}

                delete _doc.cebs;
                delete _doc.hebs;
                delete _doc.vebs;
                delete _doc.units;
                delete _doc.unit;
              }
            }

            delete _doc.updatedAt;

            delete _doc._status;

            delete _doc.__v;

            docs.push(_doc);
          });

          const headers = {};

          const data = await jsonexport(docs, {
            mapHeaders: (header) => {
              const h = header.replace(/\.|\s/g, "_").toUpperCase();

              headers[h] = "";

              return h;
            },
          });

          fs.writeFile(`./${collection.namespace}.csv`, data, (err) => {
            if (err) console.log("ERROR", collection.namespace, query);
            else
              console.log(
                "COMPLETED",
                collection.namespace,
                docs.length
                //query
              );
          });

          const dataDictionary = Object.keys(headers).map((key) => ({
            key: key,
            value: "",
          }));

          const dictionary = await jsonexport(dataDictionary, {
            mapHeaders: (header) => header.replace(/\.|\s/g, "_").toUpperCase(),
          });

          fs.writeFile(
            `./${collection.namespace}.headers.csv`,
            dictionary,
            (err) => {
              if (err)
                console.log("ERROR_HEADERS", collection.namespace, query);
              else
                console.log(
                  "COMPLETED_HEADERS",
                  collection.namespace,
                  docs.length
                  // query
                );
            }
          );
        });
      }
    });

    return;
  }
);
