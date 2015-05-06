'use strict';

var fs     = require('fs');
var byline = require('byline');

var stream = byline(fs.createReadStream('testdata/mail.log', {encoding: 'utf-8'}));

var not_parsable = 0;
stream.on('data', parse).on('end', done);

var PLUGIN = {
  postfix: postfix,
  dovecot: dovecot
};

var MATCH = {
  month: 1,
  day: 2,
  time: 3,
  hostname: 4,
  process: 5,
  log: 6
};

var DB = {
};

function parse(line) {
  /*jshint -W101*/
  var parsed = line.match(/([A-Za-z]{3,4})\s+([0-9]{1,2})\s+([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\s+([a-zA-Z0-9_-]+)\s+([a-zA-Z\/\[\]0-9-]+):\s+(.*)/);
  //                       | 1:month     |   | 2:day    |   | 3:time                         |   | 4:hostname   |   | 5:process/sub[pid]|    | 6:log|
  /*jshint +W101*/

  if (parsed) {
    var process = parsed[MATCH.process].match(/^([a-zA-Z0-9]+)\/?([a-zA-Z0-9_-]+)?(?:\[([0-9]+)\])?$/);

    if (process) {
      var service = process[1];
      if (PLUGIN[service]) {
        PLUGIN[service](parsed, process);
      }

    } else {
      console.log('cannot parse process', parsed[MATCH.process]);
    }
  } else {
    ++not_parsable;
    console.log('cannot parse', line);
  }
}

function postfix(parsed) {
  var log = parsed[MATCH.log];

  var to = !log.match(/NOQUEUE/) && log.match(/to=<([a-zA-Z0-9\@\.]+)>/);
  if (to) {
    add('to', to[1]);
  }

  var reject = log.match(/NOQUEUE/) && log.match(/to=<([a-zA-Z0-9\@\.]+)>/);
  if (reject) {
    add('reject', reject[1]);
  }

  var from = !log.match(/NOQUEUE/) && log.match(/from=<([a-zA-Z0-9\@\.]+)>/);
  if (from) {
    add('from', from[1]);
  }

  var connects = log.match(/^connect from ([a-zA-Z-_\.0-9]+)\[([0-9\.]+)\]/);
  if (connects) {
    add('connect', connects[2]);
  }
}

function dovecot(parsed) {
  var log = parsed[MATCH.log];

  var login = log.match(/^imap-login:.*user=<([a-zA-Z0-9\.@-_]+)>/);
  if (login) {
    add('imap-login', login[1]);
  }
}

function add(dbKey, value) {
  value = value.toLowerCase();
  DB[dbKey] = DB[dbKey] || {};
  DB[dbKey][value] = DB[dbKey][value] || 0;
  ++DB[dbKey][value];
}

function done() {
  console.log(DB);
  console.log('not_parsable', not_parsable);
}
