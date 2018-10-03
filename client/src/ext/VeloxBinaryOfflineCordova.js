/*global define, cordova, LocalFileSystem*/


/*global define */
; 
(function (global, factory) {
    if(!window.cordova){ return ; }
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader") ;
        module.exports = factory(VeloxScriptLoader) ;
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        console.log("Load cordova offline binary storage") ;
        global.VeloxServiceClient.setOfflineBinaryStorageEngine(factory(global.veloxScriptLoader));
    }
}(this, (function (VeloxScriptLoader) {
    'use strict';

    var storagePath = null ;
    var pathPattern = "{table}/{date}/{table_uid}_{uid}/{filename}";

    var offlineCordova = {} ;


    function mkdirs(fs, path, callback){
        var dirs = path.split("/").reverse();
        var root = fs.root;
        
        var createDir = function(dir){
            console.log("create dir ", dir) ;
            root.getDirectory(dir, {
                create : true,
                exclusive : false
            }, successCB, failCB);
        };
        
        var successCB = function(entry){
            root = entry;
            console.log("dir entry", entry) ;
            if(dirs.length > 0){
                createDir(dirs.pop());
            }else{
                callback(null, entry);
            }
        };
        
        var failCB = function(err){
            console.log("create dir error", err) ;
            callback(err) ;
        };
        
        createDir(dirs.pop());
    }

    function getFs(callback){
        window.requestFileSystem(LocalFileSystem.PERSISTENT, 0, function (fs) {
            callback(null, fs) ;
        }, function(err){
            callback(err) ;
        });
    }

    offlineCordova.prepare = function (options, callback) {
        if(options.pathPattern){
            pathPattern = options.pathPattern ;
        }
        if (!storagePath) {
            getFs(function (err, fs) {
                if(err){ return callback(err) ;}
                storagePath = "binary_storage/"+ (options.prefix || "default") ;
                mkdirs(fs, storagePath, function(err) {
                    if (err) {callback(err); }
                    callback() ;
                });
            });
        }else{
            callback() ;
        }
    };

    function checksum(buffer, callback){
        var res;
        try{
            console.log("before crypto");
            var crypto = (window.crypto||window.msCrypto) ;
            var subtle = crypto.subtle || crypto.webkitSubtle ;
            res = subtle.digest("SHA-256", buffer) ;
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

    /**
     * Create the target path from the configured pattern 
     * 
     * @param {object} binaryRecord the binary record
     */
    function createTargetPath(binaryRecord){
        var targetPath = pathPattern ;
        targetPath = targetPath.replace(new RegExp("{table}", "g"), binaryRecord.table_name || "no_table") ;
        targetPath = targetPath.replace(new RegExp("{table_uid}", "g"), binaryRecord.table_uid || "no_uid") ;
        targetPath = targetPath.replace(new RegExp("{uid}", "g"), binaryRecord.uid) ;
        targetPath = targetPath.replace(new RegExp("{filename}", "g"), binaryRecord.filename) ;
        var extname = "";
        if( binaryRecord.filename){
            var indexDot = binaryRecord.filename.lastIndexOf(".") ;
            if(indexDot !== -1){
                extname = binaryRecord.filename.substring(indexDot) ;
            }
        }
        targetPath = targetPath.replace(new RegExp("{ext}", "g"), extname) ;
        var creationDatetimeIso = binaryRecord.creation_datetime ;
        if(typeof(creationDatetimeIso) !== "string"){
            creationDatetimeIso = binaryRecord.creation_datetime.toISOString() ;
        }
        targetPath = targetPath.replace(new RegExp("{date}", "g"), creationDatetimeIso.substring(0,10)) ;
        targetPath = targetPath.replace(new RegExp("{time}", "g"), creationDatetimeIso.substring(11,19).replace(/:/g, "_")) ;
        return targetPath ;
    }

    function createRecordPath(filepath){
        return filepath + ".json" ;
    }

    function writeFile(fileEntry, buffer, callback) {
        console.log("write file ", fileEntry);
        fileEntry.createWriter(function (fileWriter) {

            fileWriter.onwriteend = function() {
                console.log("write file end", fileEntry);
                callback() ;
            };

            fileWriter.onerror = function (e) {
                console.log("write file error", fileEntry, e);
                callback(e) ;
            };

            fileWriter.write(buffer);
        });
    }

    offlineCordova.saveBinary = function(bufferOrFile, binaryRecord, callback){
        getBuffer(bufferOrFile, function(err, buffer){
            if(err){ return callback(err) ;}
            var filepath = storagePath+"/"+createTargetPath(binaryRecord) ;
            var dirname = filepath.substring(0, filepath.lastIndexOf("/")) ;
            var fileName = filepath.substring(filepath.lastIndexOf("/")) ;
            console.log("create dir "+dirname) ;
            getFs(function (err, fs) {
                if(err){ return callback(err) ;}
                mkdirs(fs, dirname, function(err){
                    if(err){ return callback(err) ;}

                    fs.root.getFile(filepath, {create: true, exclusive: false}, function(fileEntry) {
                        writeFile(fileEntry, buffer, function(err){
                            if(err){ return callback(err) ;}
                            console.log("written file ",filepath) ;
                            callback() ;
                        }) ;
                    }, function(err){
                        callback(err) ;
                    });
                });
            });
        }) ;
    } ;

    function readFile(fs, filePath, callback) {
        // Create a FileWriter object for our FileEntry (log.txt).
        fs.root.getFile(filePath, { create: false, exclusive: false }, function (fileEntry) {
            fileEntry.file(function (file) {
                var reader = new FileReader();
        
                reader.onload = function () {
                    var data = reader.result;
                    callback(null, data) ;
                };

                reader.onerror = function (e) {
                    callback(e) ;
                };
        
                reader.readAsArrayBuffer(file);
        
            }, function(err){
                return callback(err) ;
            });

        }, function(err){
            return callback(err) ;
        });
    }

    function readTxtFile(fs, filePath, callback) {
        // Create a FileWriter object for our FileEntry (log.txt).
        fs.root.getFile(filePath, { create: false, exclusive: false }, function (fileEntry) {
            fileEntry.file(function (file) {
                var reader = new FileReader();
        
                reader.onload = function () {
                    var data = reader.result;
                    callback(null, data) ;
                };

                reader.onerror = function (e) {
                    callback(e) ;
                };
        
                reader.readAsText(file);
        
            }, function(err){
                return callback(err) ;
            });

        }, function(err){
            return callback(err) ;
        });
    }


    offlineCordova.getLocalInfos = function(binaryRecord, callback){
        var filepath = storagePath+"/"+createTargetPath(binaryRecord) ;
        var recordpath = createRecordPath(filepath) ;

        
        getFs(function (err, fs) {
            if(err){ return callback(err) ;}
            readTxtFile(fs, recordpath, function(err, recordStr){
                if(err){ 
                    recordStr = null;
                }
                var lastSyncRecord = null;
                if(recordStr){
                    try {
                        lastSyncRecord = JSON.parse(recordStr) ;
                    } catch(err){
                        return callback(err) ;
                    }
                }
                readFile(fs, filepath, function(err, buffer){
                    if(err){ 
                        return callback(null, null, null) ;
                    }
                    checksum(buffer, function(err, hash){
                        if(err){ return callback(err) ;}
                        var file = new Uint8Array(buffer) ;
                        callback(null, {file: file, checksum: hash}, lastSyncRecord) ;
                    });
                }) ;
            });
        });
    } ;

    offlineCordova.getFileBuffer = function(binaryRecord, callback){
        var filepath = storagePath +"/"+ createTargetPath(binaryRecord) ;
        getFs(function (err, fs) {
            if(err){ return callback(err) ;}
            readFile(fs, filepath, function(err, buffer){
                if(err){ return callback(err) ;}
                callback(null, buffer) ;
            }.bind(this));
        });
    } ;



    offlineCordova.openFile = function(binaryRecord, filename, callback){
        var filepath = storagePath+"/"+createTargetPath(binaryRecord) ;
        console.log("open file", filepath) ;
        getFs(function(err, fs){
            if(err){ return callback(err) ;}
            fs.root.getFile(filepath, { create: false, exclusive: false }, function (fileEntry) {
                console.log('Open file: ' + filepath);
                cordova.plugins.fileOpener2.open(
                    fileEntry.toURL(), 
                    binaryRecord.mime_type, 
                    { 
                        error : function(e) { 
                            console.log('Error status: ' + e.status + ' - Error message: ' + e.message);
                            callback(e);
                        },
                        success : function () {
                            callback() ;
                        }
                    }
                );
            }, function(err){
                return callback(err) ;
            });
        });
    } ;

    offlineCordova.markAsUploaded = function(binaryRecord, callback){
        var filepath = storagePath+"/"+createTargetPath(binaryRecord) ;
        var recordpath = createRecordPath(filepath) ;
        getFs(function (err, fs) {
            if(err){ return callback(err) ;}

            var dirname = recordpath.substring(0, recordpath.lastIndexOf("/")) ;
            console.log("create dir "+dirname) ;
            mkdirs(fs, dirname, function(err){
                if(err){ return callback(err) ;}
                fs.root.getFile(recordpath, {create: true, exclusive: false}, function(fileEntry) {
                    writeFile(fileEntry, JSON.stringify(binaryRecord, null, 2), function(err){
                        if(err){ return callback(err) ;}
                        console.log("written file ",recordpath) ;
                        callback() ;
                    }) ;
                }, function(err){
                    callback(err) ;
                });
            });
        });
    } ;


    function addEntry(entries, parent, fileEntries, callback){
        if(fileEntries.length === 0){ return callback() ;}
        var fileEntry = fileEntries.pop() ;
        
        if(fileEntry.isDirectory){
            var reader = fileEntry.createReader();
            reader.readEntries(function (files) {
                addEntry(entries, parent?(parent+"/"+fileEntry.name):fileEntry.name, files, function(err){
                    if(err){ return callback(err) ;}
                    addEntry(entries, parent, fileEntries, callback) ;
                }) ;
            }, function(err){
                if(err){ return callback(err) ;}
            }) ;
        }else{

            fileEntry.file(function (file) {
                var reader = new FileReader();
        
                reader.onload = function () {
                    var data = reader.result;
                    entries.push({
                        path : parent?(parent+"/"+fileEntry.name):fileEntry.name,
                        data : data
                    }) ;
                    addEntry(entries, parent, fileEntries, callback) ;
                };

                reader.onerror = function (e) {
                    callback(e) ;
                };
        
                reader.readAsArrayBuffer(file);
        
            }, function(err){
                return callback(err) ;
            });
        }
    }

    offlineCordova.getEntries = function(callback){
        var entries = [] ;

        getFs(function (err, fs) {
            if(err){ return callback(err) ;}

            var reader = fs.createReader();
            reader.readEntries(
            function (files) {
                addEntry(entries, "", files, function(err){
                    if(err){ return callback(err) ;}
                    callback(null, entries) ;
                }) ;
            },
            function (err) {
                if(err){ return callback(err) ;}
            });
        });
    } ;

    offlineCordova.restoreEntries = function(entries, callback){

        getFs(function (err, fs) {
            if(err){ return callback(err) ;}

            var promises = [] ;
            entries.forEach(function(entry){
                promises.push(new Promise(function(resolve, reject){
                    var recordpath = entry.path;
                    var dirname = recordpath.substring(0, recordpath.lastIndexOf("/")) ;
                    console.log("create dir "+dirname) ;
                    mkdirs(fs, dirname, function(err){
                        if(err){ return callback(err) ;}
                        fs.root.getFile(recordpath, {create: true, exclusive: false}, function(fileEntry) {
                            writeFile(fileEntry, entry.data, function(err){
                                if(err){ return callback(err) ;}
                                console.log("written file ",recordpath) ;
                                resolve() ;
                            }) ;
                        }, function(err){
                            reject(err) ;
                        });
                    });
                }));
            }.bind(this)) ;

            Promise.all(promises).then(function() {
                callback() ;
            }).catch(function(err) {
                callback(err);
            });
        });
    } ;


    return offlineCordova ;
})));