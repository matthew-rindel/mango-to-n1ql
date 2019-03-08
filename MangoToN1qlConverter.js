class MangoToN1qlConverter
{
  constructor()
  {
    this.operands = new Map();
    this.operands.set("$eq", "=");
    this.operands.set("$ne", "<>");
    this.operands.set("$gt", ">");
    this.operands.set("$gte", ">=");
    this.operands.set("$lt", "<");
    this.operands.set("$lte", "<=");
    this.operands.set("$in", "IN");

    this.nullOperands = new Map();
    this.nullOperands.set("=", "IS");
    this.nullOperands.set("<>", "IS NOT");
    this.nullOperands.set(">", "IS NOT");
    this.nullOperands.set(">=", "IS NULL OR IS NOT");

    this.convertOperand = this.convertOperand.bind(this);
  }

  normaliseSelector(selector)
  {
    selector = selector || {};
    const keys = [...Object.keys(selector)];

    for (let key of keys)
    {
      if(!key.startsWith("$"))
      {
        if(typeof selector[key] === "string" ||
        typeof selector[key] === "number" ||
        typeof selector[key] === "boolean" ||
          selector[key] === null)
        {
          selector[key] = {"$eq": selector[key] };
        }
        else if(typeof selector[key] === "object")
        {
          selector[key] = this.normaliseSelector(selector[key]);
        }
      }
    }

    if (!keys.includes("$and") && keys.length > 1)
    {
      selector = { "$and": keys.map(x =>
        {
          const temp = {};
          temp[x] = selector[x];
          return temp;
        })
      };
    }

    return selector;
  }

  convertIndex(index)
  {
    if(index.selector)
    {
      const bucketName = index.bucketName;
      index = this.covertSelectorToIndex(index.selector);

      index.bucketName = bucketName;
    }

    index.index = index.index || {};
    index.index.fields = index.index.fields || [];

    const indexName = `idx_${index.index.fields.map(x => x.replace(".", "-")).join("_")}`;

    index.index.fields = index.index.fields.map(x => x.split(".").map(y => "`" + y + "`").join("."));

    return `CREATE INDEX ${indexName} ON ${index.bucketName} (${index.index.fields.join(", ")})`;
  }

  covertSelectorToIndex(selector)
  {
    let fields = [];

    if(selector["$and"])
    {
      fields = fields.concat(selector["$and"]);
    }
    else if(selector["$or"])
    {
      fields = fields.concat(selector["$and"]);
    }
    else {
      fields = fields.concat(Object.keys(selector));
    }

    const index = {
      fields: fields
    };

    return { index: index };
  }

  convertOperand(selector)
  {
    let key = Object.keys(selector)[0];
    const operandObj = selector[key];
    const operand = Object.keys(operandObj)[0];

    let value = null;

    if(operand === "$and" || operand === "$or")
    {
        return operandObj[operand].map(x =>
          {
            const newObj = {};
            newObj[key] = x;

            return newObj;
          }).map(this.convertOperand).join(operand === "$and" ? " AND " : " OR ");
    }
    else
    {
     value = operandObj[operand];
     value = typeof value === "string" ? `'${value}'` : value;

     if(operand === "$in")
     {
       value = value.map(x => typeof x === "string" ? `'${x}'` : x).join(", ");
       value = `[${value}]`
     }

     let sqlOperand = this.operands.get(operand);

     if(value === null && this.nullOperands.has(sqlOperand))
     {
       sqlOperand = this.nullOperands.get(sqlOperand);
       value = "NULL";
     }

     key = key.split(".").map(x => "`" + x + "`").join(".");

     if(sqlOperand === this.nullOperands.get(">="))
     {
      return `(${key} IS ${value} OR ${key} IS NOT ${value})`;
     }
     else
     {
      return `${key} ${sqlOperand} ${value}`;
     }
    }
  }

  createWhereClause(mangoQuery)
  {
    let where = "";

    for (const key in mangoQuery.selector)
    {
      const value = mangoQuery.selector[key];

      if(key === "$and")
      {
        const ands = value.map(this.convertOperand).join(" AND ");
        where += ands;
      }

      if(key === "$or")
      {
        const ors = value.map(this.convertOperand).join(" OR ");
        where += " " + ors;
      }

      if(key !== "$or" && key !== "$and")
      {
        where = this.convertOperand(value);
      }

      if(where)
      {
        where = `WHERE ${where}`;
      }
    }

    return where;
  }

  convertFindQueryToCount(mangoQuery)
  {
    mangoQuery.selector = mangoQuery.selector || {};
    mangoQuery.sort = mangoQuery.sort || [];
    mangoQuery.fields = mangoQuery.fields || [];

    mangoQuery.selector = this.normaliseSelector(mangoQuery.selector);

    let field = mangoQuery.fields[0];

    if(!field)
    {
      if(mangoQuery.selector["$and"])
      {
        field = Object.keys(mangoQuery.selector["$and"][0])[0];
      }
      else if(mangoQuery.selector["$or"])
      {
        field = Object.keys(mangoQuery.selector["$or"][0])[0];
      }
      else
      {
        field = Object.keys(mangoQuery.selector)[0];
      }
    }

    field = "`" + field + "`";
    let sql = `SELECT COUNT(${field}) AS docCount FROM ${mangoQuery.bucketName}`;

    const where = this.createWhereClause(mangoQuery);

    return `${sql} ${where}`.trim();
  }

  convertFindQuery(mangoQuery)
  {
    mangoQuery.selector = mangoQuery.selector || {};
    mangoQuery.sort = mangoQuery.sort || [];
    mangoQuery.fields = mangoQuery.fields || [];

    mangoQuery.selector = this.normaliseSelector(mangoQuery.selector);

    const fields = mangoQuery.fields.length === 0 ? `meta().id as _id, ${mangoQuery.bucketName}.*` : mangoQuery.fields.join(", ");

    let sql = `SELECT ${fields} FROM ${mangoQuery.bucketName}`;
    const where = this.createWhereClause(mangoQuery);

    sql = `${sql} ${where}`.trim();

    if(mangoQuery.limit)
    {
      sql = `${sql} LIMIT ${mangoQuery.limit}`;
    }

    return sql;
  }

}

module.exports = MangoToN1qlConverter;
