/**
 * Dependencies
 */

var _ = require('lodash');

/**
 * Radar Interface
 *
 * @driver {Object} - Driver to use for accessing data
 * @connectionString {String} - Connection string for given datastore
 * @connection {Object} - Active connection object
 */

var Radar = module.exports = function(options) {
  if(!(this instanceof Radar)) {
    return new Radar(options);
  }

  if(!options.driver) {
    throw new Error('Missing a driver for Radar to use.');
  }

  if(options.connectionString) {
    this.connectionString = options.connectionString;
  }

  if(options.connection) {
    this.connection = options.connection;
  }

  // Store the driver we will be using
  this.driver = options.driver;

  // Build an empty query to work with
  this.query = {};

  return this;
};


// Select
Radar.prototype.select = function(values) {
  if(!_.has(this.query, 'select')) {
    this.query.select = values;
    return this;
  }

  // If setting a * value it overrides everything else
  if(values === '*') {
    this.query.select = '*';
    return this;
  }

  // If the values are not an array, make them an array
  if(!_.isArray(values)) {
    values = [values];
  }

  var arr = _.concat(this.values || [], values);
  arr = _.uniq(arr);

  this.query.select = arr;
  return this;
};


// From
Radar.prototype.from = function(values) {

  if(!_.has(this.query.from)) {
    this.query.from = values;
    return this;
  }

  if(_.isString(this.query.from)) {
    this.query.from = values;
  }

  if(_.isPlainObject(this.query.from)) {
    this.query.from.table = values;
  }

  return this;
};


// Where
Radar.prototype.where = function(values) {
  var criteria = this.query.where || {};
  var data = _.merge(criteria, values);
  this.query.where = data;

  return this;
};


// Schema
Radar.prototype.schema = function(schema) {
  var _from = {};
  if(_.isString(this.query.from)) {
    _from = { table: this.query.from, schema: schema };
  } else if(_.isPlainObject(this.query.from)) {
    _from.schema = schema;
  }

  this.query.from = _from;
  return this;
};


// Insert
Radar.prototype.insert = function(values) {
  var inserted = this.query.insert || {};
  var data = _.merge(inserted, values);
  this.query.insert = data;

  return this;
};


// Into
Radar.prototype.into = function(value) {
  this.query.into = value;
  return this;
};

// Limit
Radar.prototype.limit = function(value) {
  this.query.limit = value;
  return this;
};

// Exec
Radar.prototype.exec = function(cb) {

  // Execute the query
  var options = { query: this.query };
  if(this.connectionString) {
    options.connectionString = this.connectionString;
  }
  else if(this.connection) {
    options.connection = this.connection;
  }

  this.driver.executeQuery(options).exec({
    error: function(err) {
      cb(err);
    },
    success: function(data) {
      cb(null, data);
    }
  });

};


// Transactions
Radar.txn = function(options, cb) {
  if(!options) { return cb(new Error('Missing Options for Radar Transaction.')); }
  if(!options.connectionString) { return cb(new Error('Missing Connection String for Radar Transaction.')); }
  if(!options.driver) { return cb(new Error('Missing Driver for Radar Transaction.')); }

  options.driver.getConnection({
    connectionString: options.connectionString
  }).exec({
    error: function(err) {
      return cb(err);
    },
    success: function(conn) {

      // Build up a version of Radar that includes the connection
      var radarRunner = function() {
        return new Radar({
          connection: conn,
          driver: options.driver
        });
      };

      // Create a rollback function
      var rollback = function(cb) {
        options.driver.rollbackTransaction({
          connection: conn
        }).exec({
          error: cb,
          success: cb
        });
      };

      // Create a commit function
      var commit = function(cb) {
        options.driver.commitTransaction({
          connection: conn
        }).exec({
          error: cb,
          success: cb
        });
      };

      // Call the begin function
      options.driver.beginTransaction({
        connection: conn
      }).exec({

        // If theres an error, rollback
        error: function(err) {
          rollback(cb);
        },

        success: function() {
          cb(null, {
            commit: commit,
            rollback: rollback,
            radar: radarRunner
          });
        }
      });

    }
  });

};
