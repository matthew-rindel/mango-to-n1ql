const MangoToSqlConverter = require("../MangoToN1qlConverter");

const expect = require("chai").expect;
const should = require("chai").should;

describe("normaliseSelector", function()
{
  it("will normalise correctly", function()
  {
    const converter = new MangoToSqlConverter();
    const result = JSON.stringify(converter.normaliseSelector({
            	"jobDate":{"$gt":null},
            	"compDate":null,
            	"startDate":null,
            	"finDate":null,
            	"currentStatusIndex":"pretreatment-ready"
            }));



    expect(result).to.equal(JSON.stringify({
      "$and": [
        {"jobDate": {"$gt": null }},
        {"compDate": { "$eq": null}},
        {"startDate": { "$eq": null}},
        {"finDate": { "$eq": null}},
        {"currentStatusIndex": { "$eq": "pretreatment-ready"	}},
      ]
    }));
  });
});

describe("convertIndex", function()
{
  it("will generate string", function()
  {
    const converter = new MangoToSqlConverter();
    const result = converter.convertIndex({
      bucketName: "jobs",
      index: { fields: ["_id", "jobDate", "startDate"] }
    });

    expect(result).to.equal("CREATE INDEX idx__id_jobDate_startDate ON jobs (`_id`, `jobDate`, `startDate`)");

  });
});

describe("convertOperand", function()
{
  it("$eq will generate string", function()
  {
    const converter = new MangoToSqlConverter();
    const newDate = new Date().toISOString();
    const result = converter.convertOperand({
      "jobDate": {
        "$eq": newDate
      }
    });

    expect(result).to.equal("`jobDate` = " + "'" + newDate + "'");

  });

  it("$gt will generate string", function()
  {
    const converter = new MangoToSqlConverter();
    const newDate = new Date().toISOString();
    const result = converter.convertOperand({
      "jobDate": {
        "$gt": newDate
      }
    });

    expect(result).to.equal("`jobDate` > " + `'${newDate}'`);

  });


  it("$eq with null will generate string", function()
  {
    const converter = new MangoToSqlConverter();
    const result = converter.convertOperand({
      "jobDate": {
        "$eq": null
      }
    });

    expect(result).to.equal("`jobDate` IS NULL");

  });

  it("$ne with null will generate string", function()
  {
    const converter = new MangoToSqlConverter();
    const result = converter.convertOperand({
      "jobDate": {
        "$ne": null
      }
    });

    expect(result).to.equal("`jobDate` IS NOT NULL");
  });
});

describe("convertFindQuery", function()
{
  it("a query will generate valid SQL", function()
  {
    const converter = new MangoToSqlConverter();
    const result = converter.convertFindQuery({
          "bucketName": "jobs",
        	"selector": {
        	"jobDate":{"$gt":null},
        	"compDate":null,
        	"startDate":null,
        	"finDate":null,
        	"currentStatusIndex":"pretreatment-ready"
        	},
        	"limit":5,
        	"execution_stats": true
        });

    expect(result).to.equal("SELECT meta().id as _id, jobs.* FROM jobs WHERE `jobDate` IS NOT NULL AND `compDate` IS NULL AND `startDate` IS NULL AND `finDate` IS NULL AND `currentStatusIndex` = 'pretreatment-ready' LIMIT 5");
  });

  it("do it all", function()
  {
    const converter = new MangoToSqlConverter();
    const findObject = {
          "bucketName": "jobs",
        	"selector": {
        	"jobDate":{"$gt":null},
        	"compDate":null,
        	"startDate":null,
        	"finDate":null,
        	"currentStatusIndex":"pretreatment-ready"
        	},
        	"limit":5,
        	"execution_stats": true
        };

    const indexObject = converter.covertSelectorToIndex(findObject.selector);

    const indexResult = converter.convertIndex(indexObject);
    const countResult = converter.convertFindQueryToCount(findObject);
    const result = converter.convertFindQuery(findObject);
  });
});

describe("convertFindQueryToCount", function()
{
  it("a query will generate valid SQL", function()
  {
    const converter = new MangoToSqlConverter();
    const result = converter.convertFindQueryToCount({
          "bucketName": "jobs",
        	"selector": {
        	"jobDate":{"$gt":null},
        	"compDate":null,
        	"startDate":null,
        	"finDate":null,
        	"currentStatusIndex":"pretreatment-ready"
        	},
        	"limit":5,
        	"execution_stats": true
        });

    expect(result).to.equal("SELECT COUNT(`jobDate`) AS docCount FROM jobs WHERE `jobDate` IS NOT NULL AND `compDate` IS NULL AND `startDate` IS NULL AND `finDate` IS NULL AND `currentStatusIndex` = 'pretreatment-ready'");
  });
});


describe("covertSelectorToIndex", function()
{
  it("will convert a mango selector into an index object", function()
  {
    const converter = new MangoToSqlConverter();
    const result = JSON.stringify(converter.covertSelectorToIndex({
        	"jobDate":{"$gt":null},
        	"compDate":null,
        	"startDate":null,
        	"finDate":null,
        	"currentStatusIndex":"pretreatment-ready"
        	}));

    expect(result).to.equal(JSON.stringify({
      index: { fields: ["jobDate", "compDate", "startDate", "finDate", "currentStatusIndex"] }
    }));
  });
});
