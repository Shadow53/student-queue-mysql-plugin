/**
 * Created by michael on 3/7/16.
 */
var mysql = require("mysql");
var Promise = require("promise");
var crypto = require("crypto");

function checkName(name){
    if (typeof name !== "string" || name.toLowerCase() === "queues") {
        return false;
    }
    else{
        var valid = /(^\w)\w+/;
        return valid.test(name);
    }
}

function RequestDB(obj) {
    if (this instanceof RequestDB){
        var that = this;

        if (!(obj.hasOwnProperty("host") && obj.hasOwnProperty("user") && obj.hasOwnProperty("password") &&
            obj.hasOwnProperty("database") && obj.hasOwnProperty("table"))) {
            throw new Error("Missing one or more of the required options: host, user, password, database, table")
        }

        if (!checkName(obj.table)){
            throw new Error("Invalid table name");
        }

        that.table = mysql.escapeId(obj.table);

        that.pool = mysql.createPool(obj);

        return Object.freeze(that);
    }
    else return new RequestDB(obj);
}

var queueTableConfig = "(" +
    "`studentid` varchar(15) NOT NULL, " +
    "`name` varchar(255) NOT NULL, " +
    "`description` varchar(1000) NOT NULL, " +
    "`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
    "PRIMARY KEY (`studentid`)" +
    ") ENGINE=MyISAM DEFAULT CHARSET=latin1";

RequestDB.prototype.reset = function(){
    var that = this;

    return new Promise(function (resolve, reject) {
        that.pool.getConnection(function (err, connection) {
            if (err) {
                reject(err);
                return;
            }
            console.log("Dropping table");
            connection.query("TRUNCATE " + that.table, function (err) {
                if (err) reject(err);
                else resolve();
            });

            connection.release();
        });
    });
};

RequestDB.prototype.add = function(request){
    var that = this;

    if (!(request.hasOwnProperty("name") && request.hasOwnProperty("id") && request.hasOwnProperty("problem"))){
        return new Promise(function (resolve, reject) {
            reject(new Error("Missing one of the required properties: name, id, problem"));
        });
    }

    return new Promise(function (resolve, reject) {
        that.pool.getConnection(function (err, connection) {
            if (err) {
                reject(err);
                return;
            }
            connection.query("SELECT * FROM " + that.table + " WHERE `studentid` = ? LIMIT 1",
                [request.id], function (err, result) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    if (result.length > 0) {
                        reject(new Error("Record with key already exists"));
                    }
                    else {
                        connection.query("INSERT INTO " + that.table + " (`studentid`, `name`, `description`) VALUES (?, ?, ?) ",
                            [request.id, request.name, request.problem], function (err, result) {
                                if (err) reject(err);
                                else resolve();
                            });
                    }

                });

            connection.release();
        });
    });
};

RequestDB.prototype.remove = function (id) {
    var that = this;
    return new Promise(function (resolve, reject) {
        that.pool.getConnection(function (err, connection) {
            if (err) {
                reject(err);
                return;
            }

            connection.query("DELETE FROM " + that.table + "WHERE `studentid` = ?", [id], function (err, result) {
                if (err) reject(err);
                else resolve();
            });

            connection.release();
        });
    });
};

RequestDB.prototype.getAll = function () {
    var that = this;

    return new Promise(function (resolve, reject) {
        that.pool.getConnection(function (err, connection) {
            if (err) {
                reject(err);
                return;
            }
            connection.query("SELECT * FROM " + that.table + " ORDER BY timestamp ASC",
                function (err, result) {
                    if (err) reject(err);
                    else resolve(result);
                });

            connection.release();
        });
    });
};

function ConfigDB(obj){
    if (this instanceof ConfigDB){
        var that = this;
        if (!(obj.hasOwnProperty("host") && obj.hasOwnProperty("user") && obj.hasOwnProperty("password") &&
            obj.hasOwnProperty("database"))) {
            throw new Error("Missing one or more of the required options: host, user, password, database")
        }

        that.table = mysql.escapeId("queues");

        that.host = obj.host;
        that.user = obj.user;
        that.password = obj.password;
        that.database = obj.database;

        // This gets set in load()
        that.queues = {};

        that.connection = mysql.createConnection(obj);

        return Object.freeze(that);
    }
    else return new ConfigDB(obj);
}

ConfigDB.prototype.createConfigTable = function(){
    var that = this;

    return new Promise(function (resolve, reject) {
        that.connection.query("CREATE TABLE IF NOT EXISTS " + that.table + " (" +
            "`name` varchar(30) NOT NULL, " +
            "`table_name` varchar(30) NOT NULL, " +
            "`hash` varchar(44) NOT NULL, " +
            "`description` varchar(1000) NULL, " +
            "PRIMARY KEY (`table_name`), " +
            "UNIQUE KEY `name` (`name`)" +
            ") ENGINE=MyISAM DEFAULT CHARSET=latin1", function (err, result) {
            if (err) reject(err);
            else {
                that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = 'admin' LIMIT 1", function (err, result) {
                    if (err) reject(err);
                    else {
                        if (result.length > 0) resolve();
                        else {
                            that.connection.query("INSERT INTO " + that.table +
                                " (`name`, `table_name`, `hash`) VALUES ('admin', 'admin', '" + hashPassword("password") + "')",
                                function (err) {
                                    if (err) reject(err);
                                    else resolve();
                                });
                        }
                    }
                });
            }
        });
    });
};

ConfigDB.prototype.addNewQueue = function(obj){
    var that = this;

    if (!(obj.hasOwnProperty("name") && obj.hasOwnProperty("password"))) {
        return new Promise(function (resolve, reject) {
            reject(new Error("Missing one or more of the required options: name, password"));
        });
    }
    else {
        return new Promise(function (resolve, reject) {
            if (!obj.hasOwnProperty("table_name") || typeof obj.table_name !== "string") {
                obj.table_name = obj.name;
            }

            if (!checkName(obj.table_name)) {
                return new Promise(function (resolve, reject) {
                    reject(new Error("Invalid table name: " + obj.table_name));
                });
            }

            that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = ? LIMIT 1",
                [obj.name], function (err, result) {
                    if (err) {
                        reject(err);
                        return;
                    }

                    if (result.length > 0) {
                        reject(new Error("Queue with name " + obj.name + " already exists"));
                        return;
                    }
                    else {
                        that.connection.query("CREATE TABLE ?? " + queueTableConfig,
                            [obj.table_name], function (err, result) {
                                if (err) {
                                    reject(err);
                                }
                                else {
                                    var hasDesc = obj.hasOwnProperty("description");

                                    var sql = "INSERT INTO " + that.table + " (`name`, `hash`, `table_name`" +
                                        (hasDesc ? ", `description`" : "") +
                                        ") VALUES (?, ?, ?" +
                                        (hasDesc ? ", ?" : "") + ") ";

                                    var inserts = [obj.name, hashPassword(obj.password), obj.table_name];
                                    if (hasDesc) {
                                        inserts.push(obj.description)
                                    }

                                    sql = mysql.format(sql, inserts);

                                    that.connection.query(sql, function (err, result) {
                                        if (err) reject(err);
                                        else {
                                            that.load();
                                            resolve();
                                        }
                                    });
                                }
                            });
                    }
                });
        });
    }
};

function updateArg(name, arg, val, isNullable, that) {
    return new Promise(function (resolve, reject) {
        if (typeof name !== "string" || (!isNullable && val !== "string")) {
            reject(new Error("Missing one of the required arguments: name, " + arg));
            return;
        }

        that.connection.query("UPDATE " + that.table + " SET ?? = ? WHERE `name` = ? LIMIT 1",
            [arg, val, name], function (err, result) {
                if (err) reject(err);
                else resolve();
            });

    });
}

function hashPassword(password) {
    var pwHash = crypto.createHash('sha256');
    pwHash.update(password);
    return pwHash.digest("base64");
}

ConfigDB.prototype.setHash = function (name, password) {
    return updateArg(name, "hash", hashPassword(password), false, this);
};

ConfigDB.prototype.setQueueName = function (oldName, newName) {
    return new Promise(function (resolve, reject) {
        var that = this;

        if (typeof oldName !== "string" || typeof newName !== "string") {
            reject(new Error("Missing valid options: oldName, newName"));
            return;
        }

        that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = ? LIMIT 1",
            [oldName], function (err, result) {
                if (result.length > 0) {
                    if (oldName == newName) {
                        resolve();
                    }
                    else if (!checkName(newName)) {
                        reject(new Error("New name does not meet requirements: alphanumeric or underscore only"));
                    }
                    else {
                        that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = ? LIMIT 1",
                            [newName], function (err, result) {
                                if (result.length === 0) {
                                    that.connection.query("UPDATE " + that.table + "SET `name` = ? WHERE `name` = ?",
                                        [newName, oldName], function (err, result) {
                                            if (err) reject(err);
                                            else resolve();
                                        });
                                }
                                else reject(new Error("Queue with name " + newName + " already exists"));
                            });
                    }
                }
                else reject(new Error("Queue with name " + oldName + " does not exist"));
            });
    });
};

ConfigDB.prototype.setDescription = function (name, desc) {
    if (desc === "") desc = null;
    return updateArg(name, "description", desc, true, this);
};

ConfigDB.prototype._getHash = function (name) {
    var that = this;
    return new Promise(function (resolve, reject) {
        if (that.queues.hasOwnProperty(name) || name === "admin") {
            that.connection.query("SELECT `hash` FROM " + that.table + " WHERE `name` = ? LIMIT 1",
                [name], function (err, result) {
                    if (err) reject(err);
                    else resolve(result[0].hash);
                });
        }
        else {
            reject(new Error("No queue found with name " + name));
        }
    });
};

ConfigDB.prototype.deleteQueue = function (name, table_name) {
    var that = this;

    return new Promise(function (resolve, reject) {
        table_name = (table_name === undefined ? name : table_name);
        that.connection.query("DROP TABLE ??", [table_name], function (err, result) {
            if (err) reject(err);
            else {
                that.connection.query("DELETE FROM " + that.table + " WHERE `name` = ?",
                    [name], function (err, result) {
                        if (err) reject(err);
                        else resolve();
                    });
            }
        });
    });
};

ConfigDB.prototype.getAllQueues = function () {
    var that = this;
    return new Promise(function (resolve, reject) {
        that.connection.query("SELECT `name`, `description` FROM " + that.table + " WHERE `name` != 'admin' ORDER BY `name` ASC",
            function (err, result) {
                if (err) reject(err);
                else resolve(result);
            });
    });
};

ConfigDB.prototype.load = function () {
    var that = this;
    return new Promise(function (resolve, reject) {
        that.connection.query("SELECT `name`, `table_name` FROM " + that.table + " WHERE `name` != 'admin' ORDER BY `name` DESC",
            function (err, result) {
                if (err) reject(err);
                else {
                    that.queues = {};
                    result.forEach(function (queue, i, arr) {
                        console.log(queue);
                        that.queues[queue.name] = new RequestDB({
                            host: that.host,
                            user: that.user,
                            password: that.password,
                            database: that.database,
                            table: queue.table_name
                        });
                    });
                    resolve();
                }
            });
    });
};

ConfigDB.prototype.validatePassword = function (queueName, password) {
    var that = this;
    return new Promise(function (resolve, reject) {
        var getHash = that._getHash(queueName);
        getHash.then(
            function (hash) {
                var newHash = hashPassword(password);
                if (hash === newHash) resolve();
                else reject(new Error("Passwords did not match"));
            },
            function (err) {
                reject(err);
            }
        );
    });
};

module.exports = ConfigDB;