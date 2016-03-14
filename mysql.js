/**
 * Created by michael on 3/7/16.
 */
var mysql = require("mysql");
var Deferred = require("promised-io/promise").Deferred;
var crypto = require("crypto");

function checkName(name){
    if (typeof name !== "string"){
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
    var defer = new Deferred();

    that.pool.getConnection(function(err, connection) {
        if (err) {
            defer.reject(err);
            return;
        }
        console.log("Dropping table");
        connection.query("DELETE FROM " + that.table, function(err){
            if (err) throw defer.reject(err);
            else defer.resolve();
        });

        connection.release();
    });

    return defer;
};

RequestDB.prototype.add = function(request){
    var that = this;
    var internalDefer = new Deferred();
    var defer = new Deferred();

    if (!(request.hasOwnProperty("name") && request.hasOwnProperty("id") && request.hasOwnProperty("problem"))){
        defer.reject(new Error("Missing one of the required properties: name, id, problem"));
    }

    that.pool.getConnection(function(err, connection){
        if (err) {
            defer.reject(err);
            return;
        }
        connection.query("SELECT * FROM " + that.table + " WHERE `studentid` = ? LIMIT 1",
            [request.id], function (err, result){
                if (err) internalDefer.reject(err);

            if (result.length > 0){
                internalDefer.reject(new Error("Record with key already exists"));
            }
            else {
                internalDefer.resolve();
            }
        });

        internalDefer.then(
            function() {
                connection.query("INSERT INTO " + that.table + " (`studentid`, `name`, `description`) VALUES (?, ?, ?) ",
                    [request.id, request.name, request.problem], function (err, result) {
                        if (err) defer.reject(err);
                        else defer.resolve();
                    });
            },
            function(err) {
                defer.reject(err);
            });

        connection.release();
    });

    return defer;
};

RequestDB.prototype.remove = function(id){
    var that = this;
    var defer = new Deferred();

    that.pool.getConnection(function(err, connection){
        if (err) {
            defer.reject(err);
            return;
        }

        connection.query("DELETE FROM " + that.table + "WHERE `studentid` = ?", [id], function(err, result){
            if (err) defer.reject(err);
            else defer.resolve();
        });

        connection.release();
    });

    return defer;
};

RequestDB.prototype.getAll = function () {
    var that = this;
    var defer = new Deferred();

    that.pool.getConnection(function (err, connection) {
        if (err) {
            defer.reject(err);
            return;
        }
        connection.query("SELECT * FROM " + that.table + " ORDER BY timestamp ASC",
            function (err, result) {
                if (err) defer.reject(err);

                else defer.resolve(result);
            });

        connection.release();
    });

    return defer;
};

function ConfigDB(obj){
    if (this instanceof ConfigDB){
        var that = this;
        if (!(obj.hasOwnProperty("host") && obj.hasOwnProperty("user") && obj.hasOwnProperty("password") &&
            obj.hasOwnProperty("database"))) {
            throw new Error("Missing one or more of the required options: host, user, password, database")
        }

        that.table = mysql.escapeId("config");

        that.host = obj.host;
        that.user = obj.user;
        that.password = obj.user;
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

    var defer = new Deferred();
    that.connection.query("CREATE TABLE IF NOT EXISTS " + that.table + " (" +
            "`name` varchar(30) NOT NULL, " +
        "`table_name` varchar(30) NOT NULL, " +
        "`hash` varchar(44) NOT NULL, " +
            "`description` varchar(1000) NULL, " +
        "PRIMARY KEY (`table_name`), " +
        "UNIQUE KEY `name` (`name`)" +
            ") ENGINE=MyISAM DEFAULT CHARSET=latin1", function (err, result) {
            if (err) defer.reject(err);
            else defer.resolve();
        });

    return defer;
};

ConfigDB.prototype.addNewQueue = function(obj){
    var that = this;

    var doesExist = new Deferred();
    var queueTableDefer = new Deferred();
    var defer = new Deferred();

    if (!(obj.hasOwnProperty("name") && obj.hasOwnProperty("password"))) {
        defer.reject(new Error("Missing one or more of the required options: name, password"));
        return;
    }
    else {
        if (!obj.hasOwnProperty("table_name") || typeof obj.table_name !== "string") {
            obj.table_name = obj.name;
        }
        
        that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = ? LIMIT 1",
            [obj.name], function (err, result){
                if (err) {
                    defer.reject(err);
                    return;
                }

                if (result.length > 0){
                    doesExist.reject(new Error("Queue with name " + obj.name + " already exists"));
                }
                else {
                    doesExist.resolve();
                }
            });

        doesExist.then(
            function() {
                that.connection.query("CREATE TABLE ?? " + queueTableConfig,
                    [obj.table_name], function (err, result) {
                        if (err) queueTableDefer.reject(err);
                        else queueTableDefer.resolve();
                    });
            },
            function(err) {
                queueTableDefer.reject(err);
            });

        queueTableDefer.then(
            function(){
                var hasDesc = obj.hasOwnProperty("description");

                var sql = "INSERT INTO " + that.table + " (`name`, `hash`, `table_name`" +
                    (hasDesc ? ", `description`" : "") +
                    ") VALUES (?, ?, ?" +
                    (hasDesc ? ", ?" : "") + ") ";

                var inserts = [obj.name, hashPassword(obj.password), obj.table_name];
                if (hasDesc) {inserts.push(obj.description)}

                sql = mysql.format(sql, inserts);

                that.connection.query(sql, function (err, result) {
                    if (err) defer.reject(err);
                    else defer.resolve();
                });
            },
            function(err){
                defer.reject(err);
            }
        );

        that.load();
    }

    return defer;
};

function updateArg(name, arg, val, that){
    var defer = new Deferred();

    if (typeof name !== "string" || typeof val !== "string"){
        defer.reject(new Error("Missing one of the required arguments: name, " + arg));
        return;
    }

    that.connection.query("UPDATE " + that.table + " SET ?? = ? WHERE `name` = ? LIMIT 1",
        [arg, val, name], function (err, result) {
            if (err) defer.reject(err);
            else defer.resolve();
        });

    return defer;
}

function hashPassword(password) {
    var pwHash = crypto.createHash('sha256');
    pwHash.update(password);
    return pwHash.digest("base64");
}

ConfigDB.prototype.setHash = function (name, password) {
    var that = this;
    return updateArg(name, "password", hashPassword(password), that);
};

ConfigDB.prototype.setQueueName = function (oldName, newName) {
    var that = this;
    var oldExist = new Deferred();
    var newExist = new Deferred();
    var defer = new Deferred();

    if (typeof oldName !== "string" || typeof newName !== "string"){
        defer.reject(new Error("Missing valid options: oldName, newName"));
        return defer;
    }

    that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = ? LIMIT 1",
        [oldName], function(err, result){
        if (result.length > 0) oldExist.resolve();
        else oldExist.reject(new Error("Queue with name " + oldName + " does not exist"));
    });

    oldExist.then(
        function(){
            that.connection.query("SELECT * FROM " + that.table + " WHERE `name` = ? LIMIT 1",
                [newName], function(err, result){
                    if (result.length === 0) newExist.resolve();
                    else newExist.reject(new Error("Queue with name " + newName + " already exists"));
                })
        },
        function(err){
            defer.reject(err);
        }
    );

    newExist.then(
        function(){
            if (!checkName(newName)){
                defer.reject(new Error("New name does not meet requirements: alphanumeric or underscore only"));
            }
            else if (oldName == newName){
                defer.resolve();
            }
            else {
                that.connection.query("UPDATE " + that.table + "SET `name` = ? WHERE `name` = ?",
                    [newName, oldName], function(err, result){
                        if (err) defer.reject(err);
                        else defer.resolve();
                    });
            }
        },
        function(err){ defer.reject(err); }
    );
    return defer;
};

ConfigDB.prototype.setDescription = function (name, desc) {
    var that = this;
    return updateArg(name, "description", desc, that);
};

ConfigDB.prototype.getHash = function (name) {
    var that = this;
    var defer = new Deferred();

    if (that.queues.hasOwnProperty(name)) {
        that.connection.query("SELECT `hash` FROM " + that.table + " WHERE `name` = ? LIMIT 1",
            [name], function (err, result) {
                if (err) defer.reject(err);
                else defer.resolve(result[0].hash);
            });
    }
    else {
        defer.reject(new Error("No queue found with name " + name));
    }

    return defer;
};

ConfigDB.prototype.deleteQueue = function (name, table_name) {
    var that = this;
    var deleteDefer = new Deferred();
    var defer = new Deferred();

    table_name = (table_name === undefined ? name : table_name);
    that.connection.query("DROP TABLE ??", [table_name], function (err, result) {
        if (err) deleteDefer.reject(err);
        else deleteDefer.resolve();
    });

    deleteDefer.then(
        function(){
            that.connection.query("DELETE FROM " + that.table + " WHERE `name` = ?",
                [name], function(err, result){
                    if (err) defer.reject(err);
                    else defer.resolve();
                });
        },
        function(err){
            defer.reject(err);
        }
    );

    return defer;
};

ConfigDB.prototype.getAllQueues = function () {
    var that = this;
    var defer = new Deferred();

    that.connection.query("SELECT `name`, `description` FROM " + that.table + " ORDER BY `name` DESC",
        function (err, result) {
            if (err) defer.reject(err);
            else defer.resolve(result);
        });

    return defer;
};

ConfigDB.prototype.load = function () {
    var that = this;
    var defer = new Deferred();

    that.connection.query("SELECT `name`, `table_name` FROM " + that.table + " ORDER BY `name` DESC",
        function (err, result) {
            if (err) defer.reject(err);
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
                defer.resolve();
            }
        });

    return defer;
};

ConfigDB.prototype.validatePassword = function (queueName, password) {
    var that = this;
    var defer = new Deferred();

    var getHash = that.getHash(queueName);
    getHash.then(
        function (hash) {
            var newHash = hashPassword(password);
            if (hash === newHash)
                defer.resolve();
            else defer.reject(new Error("Passwords did not match"));
        }
    );

    return defer;
};

module.exports = ConfigDB;