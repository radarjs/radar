var SQLBuilder = require('machinepack-sql-builder');
var Postgres = require('machinepack-postgresql');
var _ = require('lodash');

var Radar = module.exports = function(options) {
  if(!(this instanceof Radar)) {
    return new Radar(options);
  }

  if(options.connectionString) {
    this.connectionString = options.connectionString;
  }

  if(options.connection) {
    this.connection = options.connection;
  }

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


// Exec
Radar.prototype.exec = function(cb) {
  var self = this;

  SQLBuilder.generateSql({
    dialect: 'postgres',
    query: this.query
  }).exec({
    error: function(err) {
      return cb(err);
    },
    success: function(query) {

      // Reset the query
      self.query = {};

      // If there is a connection string, open a new connection
      if(self.connectionString) {
        Postgres.getConnection({
          connectionString: self.connectionString
        }).exec({
          error: cb,
          success: function(conn) {

            // Run the query
            Postgres.runQuery({
              connection: conn.client,
              query: query
            }).exec({
              error: cb,
              success: function(data) {
                // Release the connection
                Postgres.releaseConnection({
                  release: conn.release
                }).exec({
                  error: cb,
                  success: function() {
                    cb(null, data);
                  }
                });
              }
            });
          }
        });
      }

      // Handle a pre-made connection
      if(self.connection) {
        // Run the query
        Postgres.runQuery({
          connection: self.connection,
          query: query
        }).exec({
          error: cb,
          success: function(data) {
            cb(null, data);
          }
        });
      }

      // return cb(null, query);
    }
  });
};


// Transactions
Radar.txn = function(options, cb) {
  if(!options) { return cb(new Error('Missing Options for Radar Transaction.')); }
  if(!options.connectionString) { return cb(new Error('Missing Connection String for Radar Transaction.')); }

  // Open a connection
  Postgres.getConnection({
    connectionString: options.connectionString
  }).exec({
    error: function(err) {
      return cb(err);
    },
    success: function(conn) {

      // Build up a version of Radar that includes the connection
      var radarRunner = function() {
        return new Radar({
          connection: conn.client
        });
      };

      // Create a rollback function
      var rollback = function(cb) {
        Postgres.runQuery({
          connection: conn.client,
          query: 'ROLLBACK'
        }).exec({
          error: function() {
            Postgres.releaseConnection({
              release: conn.release
            }).exec(cb);
          },
          success: function() {
            Postgres.releaseConnection({
              release: conn.release
            }).exec(cb);
          }
        });
      };

      // Create a commit function
      var commit = function(cb) {
        Postgres.runQuery({
          connection: conn.client,
          query: 'COMMIT'
        }).exec({
          error: function() {
            Postgres.releaseConnection({
              release: conn.release
            }).exec(cb);
          },
          success: function() {
            Postgres.releaseConnection({
              release: conn.release
            }).exec(cb);
          }
        });
      };

      // Call the begin function
      Postgres.runQuery({
        connection: conn.client,
        query: 'BEGIN'
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
