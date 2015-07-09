'use strict';
var fs = require('fs');
load('http://cdnjs.cloudflare.com/ajax/libs/underscore.js/1.6.0/underscore-min.js');
var _ = require('lodash');

var autoCorrect = exports;
var spellcheck;
var dictForDistance;
var defs;
var MAX_DISTANCE_CHARS = 7;
var MIN_LENGTH_EXPANSION = 5;

autoCorrect.load = function(force) {
    if (!force) {
        try {
            var json = JSON.parse(fs.readFileSync(__dirname + '/../data/corpus.json').toString());
            dictForDistance = json.dictForDistance;
            defs = json.defs;
        } catch(e) {}
        if (dictForDistance) return;
    }

    var corpus = fs.readFileSync(__dirname + '/../data/corpus.txt').toString().split('\n');
    if (corpus[corpus.length - 1] === '') corpus.pop();

    dictForDistance = [{}, {}, {}];
    defs = [];
    var conflicts = {};

    corpus.map(function(entry) {
        return entry.split(',');
    }).forEach(function(entry) {
        var str = entry[0];
        if (str[0] === '/') return;

        var def = entry[entry.length - 1];
        var defIndex = _defIndex(def);
        var distance = (str.length < MAX_DISTANCE_CHARS) ? 1 : 2;

        _editsWithMaxDistance(str, distance).forEach(function(edits, distance) {
            edits.forEach(function(edit) {
                conflicts[edit] = conflicts[edit] || [];
                if (~conflicts[edit].indexOf(entry[0])) return;
                conflicts[edit].push(entry[0]);
                if (dictForDistance[distance][edit]) return;
                dictForDistance[distance][edit] = defIndex;
            });
        });
    });

    _.each(conflicts, function(arr, edit) {
        if (arr.length < 2) return;
        delete(dictForDistance[0][edit]);
        delete(dictForDistance[1][edit]);
        delete(dictForDistance[2][edit]);
    });
};

autoCorrect.save = function() {
    if (!dictForDistance) autoCorrect.load();
    fs.writeFileSync(__dirname + '/../data/corpus.json', JSON.stringify({
        defs: defs,
        dictForDistance: dictForDistance
    }));
};

autoCorrect.correct = function(str) {
    if (!dictForDistance) autoCorrect.load();

    var parts = _partsFromString(str);
    var offsets = [];
    var correctedStrs = [];
    var newStr = str;
    var diff = 0;

    parts.forEach(function(part) {
        if (part.str.length < 3) return;

        var correction;
        var edit;
        if (!correction) {
            var maxDistance = (part.str.length < MAX_DISTANCE_CHARS) ? 1 : 2;
            _.find(_editsWithMaxDistance(part.str, maxDistance), function(edits, distance) {
                edit = (dictForDistance[distance][part.str]) ? part.str : null; // Exact match of input with def
                edit = edit || _.find(edits, _findEditForThisDefFn, dictForDistance[distance]); // Match of input edit with def
                if (!edit) return;
                return correction = defs[dictForDistance[distance][edit]];
            });
        }
        if (!correction || correction === part.str) return;
        if (correction.length < MIN_LENGTH_EXPANSION && correction.length !== part.str.length) return;

        var thisDiff = correction.length - part.str.length;
        offsets.push([part.startIndex + diff, part.endIndex + diff + thisDiff]);
        newStr = newStr.substr(0, part.startIndex + diff) + correction + str.substr(part.endIndex + 1);
        diff += thisDiff;
        correctedStrs.push(edit);
    });

    if (!offsets.length) return null;

    return {
        str: newStr,
        offsets: offsets,
        correctedStrs: correctedStrs
    };
};

function _findEditForThisDefFn(str) {
    if (!~str.indexOf('*')) return;
    return this[str];
}

var _editsWithMaxDistance = autoCorrect._editsWithMaxDistance = function _editsWithMaxDistance(str, distance) {
    return Spellcheck.editsWithMaxDistance(str, distance);
};

function _correctionForStr(str) {
    var correction = defs[dictForDistance[0][str]];
    if (correction === str) return;
    if (!correction) {
        correction = defs[dictForDistance[1][str]];
    }
    if (!correction && str.length > 5) {
        correction = defs[dictForDistance[2][str]];
    }
    if (!correction) return;
}

function _defIndex(str, add) {
    var i = defs.indexOf(str);
    if (i < 0) {
        defs.push(str);
        i = defs.length - 1;
    }
    return i;
}

var WORDS_REGEX = /\w+/g;

function _partsFromString(str) {
    var matches = _matchesRegex(str, WORDS_REGEX);

    return matches.map(_partFromMatchFn);
}

function _partFromMatchFn(match) {
    return {
        str: match[0],
        startIndex: match.index,
        endIndex: match.index + match[0].length - 1
    };
}

function _matchesRegex(string, regex) {
    var match = null;
    var matches = [];

    while (match = regex.exec(string)) {
        matches.push(match);
        if (!regex.global) return matches;
    }
    return matches;
}

var Spellcheck = {};
/**
 * The following was extracted from `require('natural/lib/natural/spellcheck/spellcheck').prototype`
 * and edited to use * instead of alphabet
 */
Spellcheck.edits = function(word) {
    var alphabet = '*';
    var edits = [];
    for(var i=0; i<word.length+1; i++) {
        if(i>0) edits.push(word.slice(0,i-1)+word.slice(i,word.length)); // deletes
        if(i>0 && i<word.length+1) edits.push(word.slice(0,i-1) + word.slice(i,i+1) + word.slice(i-1, i) + word.slice(i+1,word.length)); // transposes
        for(var k=0; k<alphabet.length; k++) {
            if(i>0) edits.push(word.slice(0,i-1)+alphabet[k]+word.slice(i,word.length)); // replaces
            edits.push(word.slice(0,i)+alphabet[k]+word.slice(i,word.length)); // inserts
        }
    }
    // Deduplicate edits
    edits = edits.filter(function (v, i, a) { return a.indexOf(v) == i; });
    return edits;
};

// Returns all edits that are up to "distance" edit distance away from the input word
Spellcheck.editsWithMaxDistance = function(word, distance) {
    return this.editsWithMaxDistanceHelper(distance, [[word]]);
};

Spellcheck.editsWithMaxDistanceHelper = function(distanceCounter, distance2edits) {
    if(distanceCounter == 0) return distance2edits;
    var currentDepth = distance2edits.length-1;
    var words = distance2edits[currentDepth];
    var edits = this.edits(words[0]);
    distance2edits[currentDepth+1] = [];
    for(var i in words) {
        distance2edits[currentDepth+1] = distance2edits[currentDepth+1].concat(this.edits(words[i]));
    }
    return this.editsWithMaxDistanceHelper(distanceCounter-1, distance2edits);
};
