/*
 ----------------------------------------------------------------------------
 | ewdrestChildProcess: Child Worker Process for EWD REST Server            |
 |                                                                          |
 | Copyright (c) 2014 M/Gateway Developments Ltd,                           |
 | Reigate, Surrey UK.                                                      |
 | All rights reserved.                                                     |
 |                                                                          |
 | http://www.mgateway.com                                                  |
 | Email: rtweed@mgateway.com                                               |
 |                                                                          |
 |                                                                          |
 | Licensed under the Apache License, Version 2.0 (the "License");          |
 | you may not use this file except in compliance with the License.         |
 | You may obtain a copy of the License at                                  |
 |                                                                          |
 |     http://www.apache.org/licenses/LICENSE-2.0                           |
 |                                                                          |
 | Unless required by applicable law or agreed to in writing, software      |
 | distributed under the License is distributed on an "AS IS" BASIS,        |
 | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. |
 | See the License for the specific language governing permissions and      |
 |  limitations under the License.                                          |
 ----------------------------------------------------------------------------

  Build 4; 30 October 2014

*/

var fs = require('fs');
var os = require('os');
var events = require('events');
var client = require('ewdliteclient');
var util = require('util');

var mumps;
var database;
var mongo;
var mongoDB;

// This set of utility functions will be made available via ewd.util

var EWD = {
  hSeconds: function(date) {
    // get [current] time in seconds, adjusted to Mumps $h time
    if (date) {
      date = new Date(date);
    }
    else {
      date = new Date();
    }
    var secs = Math.floor(date.getTime()/1000);
    var offset = date.getTimezoneOffset()*60;
    var hSecs = secs - offset + 4070908800;
    return hSecs;
  },
  hDate: function(date) {
    var hSecs = EWD.hSeconds(date);
    var days = Math.floor(hSecs / 86400);
    var secs = hSecs % 86400;
    return days + ',' + secs;
  },
  getDateFromhSeconds: function(hSecs) {
    var sec = hSecs - 4070908800;
    return new Date(sec * 1000).toString();
  },
  getMemory: function() {
    var mem = process.memoryUsage();
    var memory = {
      rss: (mem.rss /1024 /1024).toFixed(2),
      heapTotal: (mem.heapTotal /1024 /1024).toFixed(2), 
      heapUsed: (mem.heapUsed /1024 /1024).toFixed(2)
    };
    memory.pid = process.pid;
    memory.modules = ewdChild.module;
    return memory;
  },
  createToken: function() {
    var result = [];
    var chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890';
    var tokenLength = 63;
    while (--tokenLength) {
      result.push(chars.charAt(Math.floor(Math.random() * chars.length)));
    }
    return result.join('');
  },
  requireAndWatch: function(path, moduleName) {
    var module = false;
    try {
      module = require(path);
      if (moduleName) ewdChild.module[moduleName] = module;
      if (module && module.services && moduleName) {
        var list = module.services();
        ewdChild.services[moduleName] = {};
        var services = ewdChild.services[moduleName];
        for (var i = 0; i < list.length; i++) {
          services[list[i]] = {};
        }
      }
      if (ewdChild.traceLevel >= 3) ewdChild.log("requireAndWatch: " + path + " loaded by process " + process.pid, 3);
      fs.watch(path, function(event, filename) {
        if (ewdChild.traceLevel >= 3) ewdChild.log(filename + ' has changed - event = ' + event + '; process: ' + process.pid, 3);
        if (event === 'change') {
          try {
            var path = require.resolve(filename);
            delete require.cache[path];
            var module = require(path);
            if (moduleName) ewdChild.module[moduleName] = module;
            if (module && module.services && moduleName) ewdChild.services[moduleName] = module.services();
            if (!module) console.log('require failed');
            if (ewdChild.traceLevel >= 3) ewdChild.log(filename + " reloaded successfully", 3);
          }
          catch (err) {
            if (ewdChild.traceLevel >= 3) ewdChild.log(path + " could not be reloaded: " + JSON.stringify(err), 3);
          }
        }
      });
    }
    catch(err) {
      if (ewdChild.traceLevel >= 2) ewdChild.log("Error in requireAndWatch - " + path + " could not be loaded", 2);
    }
    return module;
  }

};

var ewdChild = {

  log: function(message, level, clearLog) {
    if (+level <= +ewdChild.traceLevel) {
      console.log(message);
    }
    message = null;
    level = null;
  },

  module: {},
  services: {},
  getModulePath: function(application) {
    var path = ewdChild.modulePath;
    var lchar = path.slice(-1);
    if (lchar === '/' || lchar === '\\') {
      path = path.slice(0,-1);
    }
    var delim = '/';
    if (process.platform === 'win32') delim = '\\';
    path = path + delim + application + '.js';
    return path;
  },

  sendRequest: function(site, rest, callback) {
    var destinationObj = ewdChild.server[site];
    if (typeof destinationObj !== 'undefined') {
      var service = rest.params[1].split('/')[0];
      var serviceObj = ewdChild.service[service];
      var contentType = 'application/json';
      if (typeof serviceObj !== 'undefined') {

        if (serviceObj.contentType) contentType = serviceObj.contentType;

        var args = {
          host: destinationObj.host,
          port: destinationObj.port,
          ssl: destinationObj.ssl,
          appName: serviceObj.module,
          serviceName: serviceObj.service,
          params: {
            rest_url: rest.url,
            rest_path: rest.params[1],
            rest_auth: rest.auth,
            rest_method: rest.method,
            rest_site: site
          },
          secretKey: destinationObj.secretKey
        };
        for (var name in rest.query) {
          args.params[name] = rest.query[name];
        }
        args.params.accessId = destinationObj.accessId;
        if (rest.body) args.params.ewd_body = rest.body;

        client.run(args, function(error, data) {
          if (error) {
            //console.log('error: ' + JSON.stringify(error));
            var statusCode;
            var message;
            if (error.code && error.message) {
              statusCode = error.message.statusCode || 400;
              message = error.message;
            }
            else if (!error.error.statusCode) {
              statusCode = 400;
              message = error.error;
            }
            else {
              statusCode = error.error.statusCode;
              message = error.error.text;
            }
            callback({
              error: {
                statusCode: statusCode,
                restCode: 'RESTError',
                message: message
              },
              contentType: contentType
            });
          }
          else {
            callback({
              error: false,
              response: data,
              contentType: contentType
            });
          }
        });
      }
      else {
        callback({
          error: {
            statusCode: 404,
            restCode: 'InvalidRESTService',
            message: 'Invalid REST Service Specified'
          },
          contentType: contentType
        });
      }
    }
    else {
      callback({
        error: {
          statusCode: 404,
          restCode: 'InvalidRemoteServer',
          message: 'Invalid Remote Server Specified'
        },
        contentType: contentType
      });
    }
  },


  messageHandlers: {

    // handlers for incoming messages, by type

    initialise: function(messageObj) {
      var params = messageObj.params;
      // initialising this worker process
      if (ewdChild.traceLevel >= 3) ewdChild.log(process.pid + " initialise: params = " + JSON.stringify(params), 3);
      ewdChild.ewdGlobalsPath = params.ewdGlobalsPath;
      ewdChild.startTime = params.startTime;
      ewdChild.database = params.database;
      ewdChild.traceLevel = params.traceLevel;
      ewdChild.homePath = params.homePath;
      var hNow = params.hNow;
      ewdChild.modulePath = params.modulePath;
      ewdChild.server = params.server;
      ewdChild.service = params.service;
      ewdChild.extensionModule = params.extensionModule;
      ewdChild.destination = params.destination;
      mumps = require(ewdChild.ewdGlobalsPath);
      if (ewdChild.extensionModule !== '') {
        try {
          var path = ewdChild.getModulePath(ewdChild.extensionModule);
          ewdChild.extend = EWD.requireAndWatch(path);
        }
        catch(err) {
          console.log('*** Unable to load extension module ' + ewdChild.extensionModule);
        }
      }
      if (ewdChild.database.type === 'mongodb') ewdChild.database.nodePath = 'mongoGlobals';
      var globals;
      try {
        globals = require(ewdChild.database.nodePath);
      }
      catch(err) {
        console.log("**** ERROR: The database gateway module " + ewdChild.database.nodePath + ".node could not be found or loaded");
        console.log(err);
        process.send({
          pid: process.pid, 
          type: 'firstChildInitialisationError'
        });
        return;
      }
      var dbStatus;
      if (ewdChild.database.type === 'cache') {
        database = new globals.Cache();
        dbStatus = database.open(ewdChild.database);
        if (dbStatus.ErrorMessage) {
          console.log("*** ERROR: Database could not be opened: " + dbStatus.ErrorMessage);
          if (dbStatus.ErrorMessage.indexOf('unexpected error') !== -1) {
            console.log('It may be due to file privileges - try starting using sudo');
          }
          else if (dbStatus.ErrorMessage.indexOf('Access Denied') !== -1) {
            console.log('It may be because the Callin Interface Service has not been activated');
            console.log('Check the System Management Portal: System - Security Management - Services - %Service Callin');
          }
          process.send({
            pid: process.pid, 
            type: 'firstChildInitialisationError'
          });
          return;
        }
      }
      else if (ewdChild.database.type === 'gtm') {
        database = new globals.Gtm();
        dbStatus = database.open();
        if (dbStatus && dbStatus.ok !== 1) console.log("**** dbStatus: " + JSON.stringify(dbStatus));
        ewdChild.database.namespace = '';
        var node = {global: '%zewd', subscripts: ['nextSessid']}; 
        var test = database.get(node);
        if (test.ok === 0) {
          console.log('*** ERROR: Global access test failed: Code ' + test.errorCode + '; ' + test.errorMessage);
          if (test.errorMessage.indexOf('GTMCI') !== -1) {
            console.log('***');
            console.log('*** Did you start EWD.js using "node ewdStart-gtm gtm-config"? ***');
            console.log('***');
          } 
          process.send({
            pid: process.pid, 
            type: 'firstChildInitialisationError'
          });
          return;
        }
      }
      else if (ewdChild.database.type === 'mongodb') {
        mongo = require('mongo');
        mongoDB = new mongo.Mongo();
        database = new globals.Mongo();
        dbStatus = database.open(mongoDB, {address: ewdChild.database.address, port: ewdChild.database.port});
        ewdChild.database.namespace = '';
      }
      if (ewdChild.database.also && ewdChild.database.also.length > 0) {
        if (ewdChild.database.also[0] === 'mongodb') {
          mongo = require('mongo');
          mongoDB = new mongo.Mongo();
          mongoDB.open({address: ewdChild.database.address, port: ewdChild.database.port});
        }
      }
      mumps.init(database);

      // ********************** Load Global Indexer *******************
      try {
        var path = ewdChild.getModulePath('globalIndexer');
        var indexer = EWD.requireAndWatch(path);
        indexer.start(mumps);
        if (ewdChild.traceLevel >= 2) ewdChild.log("** Global Indexer loaded: " + path, 2);
      }
      catch(err) {}
      // ********************************************************
  
      var zewd = new mumps.GlobalNode('zewd', ['ewdjs', ewdChild.httpPort]);
      
      if (params.no === 0) {
        // first child process that is started clears down persistent stored EWD.js data
        ewdChild.log("First child process (' + process.pid + ') initialising database...");
        //var funcObj;
        //var resultObj;
        var pczewd = new mumps.Global('%zewd');
        pczewd.$('relink')._delete();
        pczewd = null;
  
        zewd._delete();

        // **** Synchronise server and services between database and config file ******

        zewd = new mumps.GlobalNode('zewdREST', []);
        var zewdServer = zewd.$('server');
        var zewdServers = zewdServer._getDocument();
        var zewdService = zewd.$('service');
        var zewdServices = zewdService._getDocument;
        var name;
        for (name in ewdChild.server) {
          if (!zewdServers[name]) {
            zewdServer.$(name)._setDocument(ewdChild.server[name]);
          }
        }
        for (name in zewdServers) {
          if (!ewdChild.server[name]) {
            ewdChild.server[name] = zewdServer.$(name)._getDocument();
          }
        }
        for (name in ewdChild.service) {
          if (!zewdServices[name]) {
            zewdService.$(name)._setDocument(ewdChild.service[name]);
          }
        }
        for (name in zewdServices) {
          if (!ewdChild.service[name]) {
            ewdChild.service[name] = zewdService.$(name)._getDocument();
          }
        }

        process.send({
          pid: process.pid, 
          type: 'firstChildInitialised',
          release: true
        });
      }
      //var mem = EWD.getMemory();
      //console.log('memory: ' + JSON.stringify(mem, null, 2));
      //zewd.$('processes').$(process.pid)._value = EWD.getDateFromhSeconds(hNow);
      //console.log('hNow set for ' + process.pid + ': ' + hNow);
      zewd = null;
      process.send({
        pid: process.pid,
        type: 'initialise',
        release: true,
        empty: true
      });
    },
    //  ** end of initialise function

    exit: function(messageObj) {
      console.log('child process ' + process.pid + ' signalled to exit');
      setTimeout(function() {
        process.exit(1);
      },500);
      process.send({
        pid: process.pid, 
        type: 'exit'
      });
    },

    getConfig: function(messageObj) {
      process.send({
        type: 'getConfig',
        pid: process.pid,
        config: {
          server: ewdChild.server,
          service: ewdChild.service
        },
        contentType: 'application/json',
        release: true
      });
    },

    restRequest: function(messageObj) {

      //console.log('restRequest - ewdChild.server: ' + JSON.stringify(ewdChild.server, null, 2));
      //console.log('restRequest - ewdChild.service: ' + JSON.stringify(ewdChild.service, null, 2));

      var contentType = 'application/json';
      var rest = messageObj.rest;
      var destination = rest.params[0];

      var sendRESTRequest = function(destination, rest, ewd) {
        //console.log('sendRESTRequest - ewdChild.server: ' + JSON.stringify(ewdChild.server, null, 2));
        //console.log('sendRESTRequest - ewd.server: ' + JSON.stringify(ewd.server, null, 2));
        var allResponse = {};
        //if (destination === '_all') {
        if (ewdChild.destination[destination]) {
          //console.log('_all');
          var count = 0;
          var max = 0;
          //for (var site in ewdChild.server) {
          for (var site in ewdChild.destination[destination]) {
            max++;
          }
          //console.log('max = ' + max);
          //for (var site in ewdChild.server) {
          for (var site in ewdChild.destination[destination]) {
            //console.log('site: ' + site + '; count = ' + count);
            (function(site, rest, max, ewd) {
              ewdChild.sendRequest(site, rest, function(responseObj) {
                count++;
                //console.log('in callback - site: ' + site + '; count = ' + count);
                if (responseObj.error) {
                  allResponse[site] = {
                    error: true,
                    statusCode: responseObj.statusCode,
                    restCode: responseObj.restCode,
                    message: responseObj.message
                  }
                }
                else {
                  allResponse[site] = responseObj.response
                }
                if (count === max) {
                  console.log('sending allResponse');
                  if (ewdChild.extend.post && ewd) {
                    console.log('*** post situation 1');
                    allResponse = ewdChild.extend.post(allResponse, ewd);
                    if (!allResponse) return; // Post function has sent response to master process itself
                  }
                  process.send({
                    pid: process.pid,
                    type: 'restRequest',
                    error: false,
                    response: allResponse,
                    release: true,
                    contentType: responseObj.contentType
                  });
                }
              });
            }(site, rest, max, ewd));
          }
          return;
        }

        (function(ewd) {
          ewdChild.sendRequest(destination, rest, function(responseObj) {
            if (responseObj.error) {
              var error = responseObj.error;
              process.send({
                pid: process.pid,
                type: 'restRequest',
                error: {
                  statusCode: error.statusCode,
                  restCode: error.restCode,
                  message: error.message
                },
                release: true,
                contentType: responseObj.contentType
              });
            } 
            else {
              if (ewdChild.extend.post && ewd) {
                console.log('*** post situation 2');
                responseObj.response = ewdChild.extend.post(responseObj.response, ewd);
              }
              // the post process may do this itself if necessary

              if (responseObj.response) {
                process.send({
                  pid: process.pid,
                  type: 'restRequest',
                  error: false,
                  response: responseObj.response,
                  release: true,
                  contentType: responseObj.contentType
                });
              }
            }
          });
        }(ewd));
      };

      if (ewdChild.extend && ewdChild.extend.pre) {
        var params = {
          restRequest: rest,
          sendRequest: ewdChild.sendRequest,
          mumps: mumps,
          db: database,
          util: EWD,
          server: ewdChild.server,
          service: ewdChild.service,
          client: client
        };
        (function(params, destination) {
          ewdChild.extend.pre(params, function(response) {
            if (response) {
              if (response.error) {
                process.send({
                  pid: process.pid,
                  type: 'restRequest',
                  error: response.error,
                  release: true,
                  contentType: contentType
                });
              }
              else {
                if (ewdChild.extend.post) {
                  console.log('*** post situation 3');
                  response = ewdChild.extend.post(response, params);
                  if (!response) return; // Post function has sent response to master process itself
                }
                process.send({
                  pid: process.pid,
                  type: 'restRequest',
                  error: false,
                  response: response,
                  release: true,
                  contentType: contentType
                });
              }
            }
            else {
              // behave as normal
              sendRESTRequest(destination, params.restRequest, params);
            }
          });
        }(params, destination));
        return;
      }

      sendRESTRequest(destination, rest);
    },

    getMemory: function(messageObj) {
      messageObj = null;
      process.send({
        type: 'getMemory',
        pid: process.pid,
        memory: EWD.getMemory(),
        contentType: 'application/json',
        release: true
      });
    },

    getServer: function(messageObj) {
      var name = messageObj.content.name;
      var server = new mumps.GlobalNode('zewdREST', ['server', name]);
      var serverObj = {};
      serverObj[name] = server._getDocument();
      process.send({
        type: 'getServer',
        pid: process.pid,
        server: serverObj,
        contentType: 'application/json',
        release: true
      });
      server = null;
      messageObj = null;
      serverObj = null;
    },

    setServer: function(messageObj) {
      var content = messageObj.content;
      var server = new mumps.GlobalNode('zewdREST', ['server', content.name]);
      server._setDocument(content.data);
      process.send({
        type: 'setServer',
        pid: process.pid,
        name: content.name,
        release: true
      });
      server = null;
      messageObj = null;
    },

    updateServer: function(messageObj) {
      var name = messageObj.content.name;
      var server = new mumps.GlobalNode('zewdREST', ['server', name]);
      ewdChild.server[name] = server._getDocument();
      process.send({
        type: 'updateServer',
        pid: process.pid,
        ok: true,
        release: true
      });
      server = null;
      messageObj = null;
    },

    getSites: function(messageObj) {
      messageObj = null;
      var sites = [];
      for (var name in ewdChild.server) {
        sites.push(name);
      }
      process.send({
        type: 'getSites',
        pid: process.pid,
        sites: sites,
        contentType: 'application/json',
        release: true
      });
    },

    'EWD.resetPassword': function(messageObj) {
      var zewd = new mumps.GlobalNode('zewd', ['ewdjs', ewdChild.httpPort]);
      zewd.$('management').$('password')._value = messageObj.password;
      var sessions = new mumps.GlobalNode('%zewdSession',['session']);
      sessions._forEach(function(sessid, session) {
        session.$('ewd_password')._value = messageObj.password;
      });
      return {ok: true};
    },

    'EWD.setParameter': function(messageObj) {
      if (messageObj.name === 'monitorLevel') {
        ewdChild.traceLevel = messageObj.value;
      }
      if (messageObj.name === 'logTo') {
        ewdChild.logTo = messageObj.value;
      }
      if (messageObj.name === 'changeLogFile') {
        ewdChild.logFile = messageObj.value;
      }
      return {ok: true};
    },

  }
};

// Handle incoming messages

process.on('message', function(messageObj) {
  if (ewdChild.traceLevel >= 3) ewdChild.log('child process ' + process.pid + ' received message:' + JSON.stringify(messageObj, null, 2), 3);
  var type = messageObj.type;
  if (ewdChild.messageHandlers[type]) {
    ewdChild.messageHandlers[type](messageObj);
  }
  else if (ewdChild.extend && ewdChild.extend.messageHandlers && ewdChild.extend.messageHandlers[type]) {
    var params = {
      mumps: mumps,
      db: database,
      client: client,
      util: EWD,
      server: ewdChild.server,
      service: ewdChild.service
    }
    ewdChild.extend.messageHandlers[type](messageObj, params);
  }
  else {
    process.send({
      type: 'error',
      error: 'Message type (' + type + ') not recognised',
      pid: process.pid,
      release: true
    });
  }
  messageObj = null;
});

// Child process shutdown handler - close down database cleanly

process.on('exit',function() {
  if (database) {
    try {
      database.close();
    }
    catch(err) {}
  }
  if (ewdChild.traceLevel >= 2) ewdChild.log('*** ' + process.pid + ' closed ' + ewdChild.database.type, 2);
  if (ewdChild.database.also && ewdChild.database.also.length > 0) {
    if (ewdChild.database.also[0] === 'mongodb') {
      if (mongoDB) mongoDB.close();
    }
  }
});

// OK ready to go!

console.log('Child process ' + process.pid + ' has started');

// kick off the initialisation process now that the Child Process has started

process.send({
  type: 'childProcessStarted', 
  pid: process.pid
});



