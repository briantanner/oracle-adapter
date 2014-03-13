'use strict';

var oracle = require('oracle'),
  inherits = require('inherits'),
  nodefn = require('when/node/function'),
  EventEmitter = require('events').EventEmitter,
  Readable = require('stream').Readable,
  _ = require('underscore');

var adapter = exports;

adapter.name = 'oracle';

adapter.createQuery = function (text, params, callback) {
  if (text instanceof OracleQuery) {
    return text;
  }
  return new OracleQuery(text, params, callback);
};


/**
 * @param opts
 * @param callback
 * @returns {*}
 */
adapter.createConnection = function (opts, orcl, callback) {
  var oc;

  if (typeof orcl === 'function') {
    callback = orcl;
    orcl = oracle;
  }
  oracle = orcl || oracle;
  oc = new OracleConnection(oracle);

  oc.connect(opts)
  .then(function(connection) {
    oc.emit('open');
    oc.connection = connection;  //TODO this feels wrong
    if (callback) {
      callback(null, oc);
    }
  }, function(err) {
    return callback ? callback(err) : oc.emit('error', err);
  });
};


/**
 *
 * @param opts
 * @returns {*}
 * @constructor
 */
inherits(OracleConnection, EventEmitter);
function OracleConnection (oracle) {
  this.oracle = oracle;
}

OracleConnection.prototype.connect = function(opts) {
  return nodefn.call(this.oracle.connect.bind(this.oracle, opts));
};


/**
 *
 * @param text
 * @param params
 * @param callback
 * @returns {*}
 */
OracleConnection.prototype.query = function (text, params, callback) {
  var query = adapter.createQuery(text, params, callback);

  this.emit('query', query);

  // connection.setPrefetchRowCount(count): configures the prefetch row count for the connection. Prefetching can have a dramatic impact on performance but uses more memory.

  query.reader = this.connection.reader(text, params);

  query.readRows();
};

inherits(OracleQuery, Readable);
function OracleQuery (text, params, callback) {
  Readable.call(this, {objectMode: true});  // objectMode = stream.read(n) returns a single value instead of a Buffer of size n;
  this.text = text;
  this._fields = null;
  this._result = { rows: [] };
  this._errored = false;
  if (typeof params === 'function') {
    callback = params;
    params = [];
  }
  this.params = params || [];
  if (this.callback = callback) {
    var self = this;
    this.on('error', this.callback).on('data', function (row) {
      self._result.rows.push(row);
    });
  }
}

OracleQuery.prototype.onRow = function(row) {
  if (this._errored) { return; }
  this._gotData = true;

  if (!this._result.fields) {
    this._result.fields = Object.keys(row).map(function (name) {
      return { name: name };
    });
    this.emit('fields', this._result.fields);
  }
  this.push(row);
};

OracleQuery.prototype.readRows = function() {
  var self = this;
  return nodefn.call(this.reader.nextRow.bind(this.reader))
  .then(function (row) {
    if (!row || _.isEmpty(row)) {
      return self.callback(null, self._result);
    }
    self.onRow(row);
    self.readRows();
  }, function (err) {
    if (err) {
      self._errored = true;
      self.emit('close');
      self.emit('error', err);
      self.callback(err, null);
    }
  })
  .done();
};

OracleQuery.prototype._read = function() {};




