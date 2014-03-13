'use strict';

var should = require('should'),
  adapter = require('../lib/index'),
  oracle = require('oracle'),
  sinon = require('sinon'),
  when = require('when'),
  nodefn = require('when/node/function');

/*global describe, it, afterEach */
/*jshint expr: true, unused:false */

function getOracleMock(connectData, response, err) {
  /*
  var mOracle = sinon.mock(oracle);
  mOracle.expects('connect').once().withArgs(connectData).yields(null, {
    execute: function(text, params, cb) {

      setTimeout(function(cb) {
        cb = cb || function() {};
        console.log('calling the freaking callback now', response);
        cb(null, response); }, 10);
    },
    close: function() {}
  });
  return mOracle;
  */
  return oracle;
}

describe('oracle tests', function() {

  afterEach(function (done) {
    if (oracle.connect.restore) {
      oracle.connect.restore();
    }
    done();
  });

  describe('connections and queries', function() {

    it ('should have an adapter', function() {
      adapter.should.exist;
    });

    it ('should return a  connection', function(done) {
      var connectData = { "hostname": "dink.nf.homes.com", "user": "QuinnR", "password": "notexpired7", "database": "ols" },
        mOracle = getOracleMock(connectData, 'response');

      nodefn.call(adapter.createConnection.bind(adapter, connectData, oracle))
      .then(function(conn) {
        mOracle.verify();
      })
      .done(done, done);
    });

    it ('should execute a query', function (done) {
      var connectData = { "hostname": "dink.nf.homes.com", "user": "QuinnR", "password": "notexpired7", "database": "ols" },
        mOracle = getOracleMock(connectData, 'response');
      nodefn.call(adapter.createConnection.bind(adapter, connectData))
      .then(function(conn) {
        return nodefn.call(conn.query.bind(conn, "SELECT * from portal_mh_account where email = 'nrvagent99@gmail.com'", []))
        .then(function(results) {
          if (conn.connection.isConnected()) {
            conn.connection.close();
          }
          should.exist(response);
        })
        .catch(console.log)
        .finally(function() {
          done();
        });
      });

    });

    it ('should work with a stored procedure', function (done) {
      var connectData = { "hostname": "dink.nf.homes.com", "user": "QuinnR", "password": "notexpired7", "database": "ols" },
        mOracle = getOracleMock(connectData, 'response'),
        response;
      nodefn.call(adapter.createConnection.bind(adapter, connectData))
        .then(function(conn) {
          return nodefn.call(conn.query.bind(conn, "call LEAD_MANAGEMENT.Select_Lead_Email_Addresses(:1, :2, :3, :4)",
              [152625333, "portal", new oracle.OutParam(oracle.OCCICURSOR), new oracle.OutParam(oracle.OCCISTRING, {size: 40})]))
            .then(function(results) {
              response = results;
              console.log('results!', results);
//              if (conn.connection.isConnected()) {
//                conn.connection.close();
//              }
            })
            .catch(console.log)
            .finally(function() {
              //should.exist(response);
              console.log('is it connected?', conn.connection.isConnected());

              if (conn.connection.isConnected()) {
                console.log('closing the connection');
                conn.connection.close();
                console.log('is it connected?', conn.connection.isConnected());
              }
              done();
            });
        })
    });

    it ('stream with query', function (done) {
      var connectData = { "hostname": "dink.nf.homes.com", "user": "QuinnR", "password": "notexpired7", "database": "ols" };
      oracle.connect(connectData, function(err, connection) {
        connection.setPrefetchRowCount(50);
        var reader = connection.reader("SELECT * from portal_mh_account where email = 'nrvagent99@gmail.com'", []);

        function doRead(cb) {
          reader.nextRow(function(err, row) {
            if (err) return cb(err);
            if (row) {
              // do something with row
              console.log("got " + JSON.stringify(row));
              // recurse to read next record
              return doRead(cb)
            } else {
              // we are done
              done();
              return cb();
            }
          })
        }

        doRead(function(err) {
          if (err) return console.log('errrrrr', err); // or log it
          console.log("all records processed");
        });

      });
    });

    it.only ('stream with stored procedure', function (done) {
      var connectData = { "hostname": "dink.nf.homes.com", "user": "QuinnR", "password": "notexpired7", "database": "ols" };
      oracle.connect(connectData, function(err, connection) {
        connection.setPrefetchRowCount(50);
        var reader = connection.reader("call LEAD_MANAGEMENT.Select_Lead_Email_Addresses(:1, :2, :3, :4)",
          [152625333, "portal", new oracle.OutParam(oracle.OCCICURSOR), new oracle.OutParam(oracle.OCCISTRING, {size: 40})]);

        function doRead(cb) {
          reader.nextRow(function(err, row) {
            console.log('computer, verify that we are connected to the database\n', connection.isConnected() ?  'affirmative, we are connected' : 'negative, we lost the connection');
            if (err) return cb(err);
            if (row) {
              // do something with row
              console.log("got " + JSON.stringify(row));
              // recurse to read next record
              return doRead(cb)
            } else {
              // we are done
              done();
              return cb();
            }
          })
        }

        doRead(function(err) {
          if (err) {
            console.log('computer, verify that we are connected to the database\n', connection.isConnected() ?  'affirmative, we are connected' : 'negative, we lost the connection');
            connection.close()
            console.log(connection.isConnected());
            done();
            return console.log('sir, we have a problem, could you take a look at this:', err);
          } // or log it
          console.log("all records processed");
        });
      });
    });

    it ('stored procedure with callback', function (done) {
      var connectData = { "hostname": "dink.nf.homes.com", "user": "QuinnR", "password": "notexpired7", "database": "ols" };
      oracle.connect(connectData, function(err, connection) {
        connection.execute("call LEAD_MANAGEMENT.Select_Lead_Email_Addresses(:1, :2, :3, :4)", [156233549, "portal",
          new oracle.OutParam(oracle.OCCICURSOR), new oracle.OutParam(oracle.OCCISTRING, {size: 40})], function(err, results) {
          if (err) {console.log('error!', err);}
          else {console.log('results:', results);}
          // console.log(results[0]['DATE_CREATED'].getTimezoneOffset());
          connection.close(); // call this when you are done with the connection
          done();
        });
      });
    });


  });

});