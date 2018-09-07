/*global define */
; (function (global, factory) {
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader") ;
        module.exports = factory(VeloxScriptLoader) ;
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        console.log("Load forage offline binary storage") ;
        global.VeloxServiceClient.setOfflineBinaryStorageEngine(factory(global.veloxScriptLoader));
    }
}(this, (function (VeloxScriptLoader) {
    'use strict';

    var LOCALFORAGE_VERSION = "1.7.2";
    var LIE_VERSION = "3.3.0";

    var LOCALFORAGE_LIB = [
        {
            name: "lie-polyfill",
            type: "js",
            version: LIE_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lie/$VERSION/lie.polyfill.min.js",
            bowerPath: "lie/dist/lie.polyfill.min.js",
            npmPath: "lie/dist/lie.polyfill.min.js"
        },
        {
            name: "localforage",
            type: "js",
            version: LOCALFORAGE_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/localforage/$VERSION/localforage.nopromises.min.js",
            bowerPath: "localforage/dist/localforage.nopromises.min.js",
            npmPath: "localforage/dist/localforage.nopromises.min.js"
        }
    ];

    /**
     * @typedef VeloxDbOfflineLokiOptions
     * @type {object}
     * @property {string} [prefix] prefix for storage name
     */

    /**
     * The Velox database loki engine
     * 
     * @constructor
     * 
     * @param {VeloxDbOfflineLokiOptions} options database client options
     */
    function VeloxBinaryOfflineForage() {
        this.path = null;
    }

    VeloxBinaryOfflineForage.prototype.prepare = function (options, callback) {
        this.options = options ;
        if (!this.path) {
            this.path = (options.prefix || "") + "velox-binary-offline";
        }
        this.importLibIfNeeded(function (err) {
            if (err) { return callback(err); }
            this.localforage.config({
                name: (options.prefix || "") + "velox-binary-offline"
            });
            callback() ;
        }.bind(this));
    };

    VeloxBinaryOfflineForage.prototype.importLibIfNeeded = function (callback) {
        if (!this.localforage) {
            if (!VeloxScriptLoader) {
               return console.error("To have automatic script loading, you need to import VeloxScriptLoader");
            }

            VeloxScriptLoader.load(LOCALFORAGE_LIB, function (err) {
                if (err) { return callback(err); }
                this.localforage = window.localforage;
                callback();
            }.bind(this));
        } else {
            callback();
        }
    };

    function checksum(buffer, callback){
        var res;
        try{
            console.log("before crypto");
            res = (window.crypto||window.msCrypto).subtle.digest("SHA-256", buffer) ;
            console.log("after crypto compl ?", res);
        }catch(e){
            callback(e) ;
        }
        if (res.then) {
            console.log("after crypto is then");
            res.then(function(buf){
                var hash = Array.prototype.map.call(new Uint8Array(buf), function(x){ return (('00'+x.toString(16)).slice(-2)); } ).join('');
                callback(null, hash) ;
            }).catch(function(err){ callback(err) ;});
        } else {    // IE11
            console.log("after crypto is NOT THEN");
            res.oncomplete=function() { // operation is complete
                var hash = Array.prototype.map.call(new Uint8Array(res.result), function(x){ return (('00'+x.toString(16)).slice(-2)); } ).join('');
                callback(null, hash) ;
            };
        }
        
    }

    function getBuffer(blobOrFile, callback){
        var hasArrayBuffer = typeof ArrayBuffer === 'function';
        var isArrayBuffer = hasArrayBuffer && (blobOrFile instanceof ArrayBuffer || toString.call(blobOrFile) === '[object ArrayBuffer]');
        if(isArrayBuffer){
            return callback(null, blobOrFile) ;
        }

        var reader = new FileReader();

        reader.onerror = function (e) {
            callback(e) ;
        };

        reader.onload = function () {
            var data = reader.result;
            callback(null, data) ;
        };

        reader.readAsArrayBuffer(blobOrFile);
    }

    function fileKey(binaryRecord){
        return "bin-file-"+binaryRecord.uid ;
    }
    function recordKey(binaryRecord){
        return "bin-record-"+binaryRecord.uid ;
    }

    VeloxBinaryOfflineForage.prototype.saveBinary = function(blobOrFile, binaryRecord, callback){
        getBuffer(blobOrFile, function(err, buffer){
            if(err){ return callback(err) ;}
            this.localforage.setItem(fileKey(binaryRecord), buffer, function(err){
                if(err){ return callback(err) ;}
                callback() ;
            }.bind(this));
        }.bind(this)) ;
    } ;

    VeloxBinaryOfflineForage.prototype.getLocalInfos = function(binaryRecord, callback){
        this.localforage.getItem(recordKey(binaryRecord), function(err, lastSyncRecord){
            if(err){ return callback(err) ;}
            this.localforage.getItem(fileKey(binaryRecord), function(err, buffer){
                if(err){ return callback(err) ;}
                if(!buffer){
                    //no local buffer, this record does not exists
                    return callback(null, null, null) ;
                }
                checksum(buffer, function(err, hash){
                    if(err){ return callback(err) ;}
                    var file = buffer ;
                    callback(null, {file: file, checksum: hash}, lastSyncRecord) ;
                }.bind(this));
            }.bind(this));
        }.bind(this));
    } ;
    
    VeloxBinaryOfflineForage.prototype.getFileBuffer = function(binaryRecord, callback){
        this.localforage.getItem(fileKey(binaryRecord), function(err, buffer){
            if(err){ return callback(err) ;}
            callback(null, buffer) ;
        }.bind(this));
    } ;
    VeloxBinaryOfflineForage.prototype.openFile = function(binaryRecord, filename, callback){
        this.localforage.getItem(recordKey(binaryRecord), function(err, binaryRecord){
            if(!binaryRecord){
                return callback("file not found") ;
            }
            if(err){ return callback(err) ;}
            this.localforage.getItem(fileKey(binaryRecord), function(err, buffer){
                if(err){ return callback(err) ;}
                var blob = new Blob( [ buffer ], { type: binaryRecord.mime_type } );
                var link = document.createElement('a');
                link.href = window.URL.createObjectURL(blob);
                link.download = filename || binaryRecord.filename;
                document.body.appendChild(link) ;
                link.click();
                callback(null) ;
                setTimeout(function(){
                    document.body.removeChild(link) ;
                }, 10) ;
            }.bind(this));
        }.bind(this));
    } ;

    VeloxBinaryOfflineForage.prototype.markAsUploaded = function(binaryRecord, callback){
        this.localforage.setItem(recordKey(binaryRecord), binaryRecord, function(err){
            if(err){ return callback(err) ;}
            callback() ;
        }.bind(this));
    } ;

    return new VeloxBinaryOfflineForage();
})));