/**
 * Created by michael on 3/7/16.
 */
var mysql = require("mysql");
var Deferred = require("promised-io/promise").Deferred;

function checkName(name){
    if (typeof name !== "string"){
        if (name !== undefined){
            return false;
        }
    }
    else{
        var valid = /(^\w)\w+/;
        if (!valid.test(name)){
            return false;
        }
    }

    return true;
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
    var internalDefer = new Deferred();
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
            obj.hasOwnProperty("database") && obj.hasOwnProperty("table"))) {
            throw new Error("Missing one or more of the required options: host, user, password, database, table")
        }

        if (!checkName(obj.table)){
            throw new Error("Invalid table name");
        }

        that.table = mysql.escapeId(obj.table);

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
        "`password` varchar(44) NOT NULL, " +
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

    if (!(obj.hasOwnProperty("name") && obj.hasOwnProperty("passwordHash"))){
        defer.reject(new Error("Missing one or more of the required options: name, passwordHash"));
        return;
    }
    else {
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
                var tableName;
                if (typeof obj.tableName === "string")
                    tableName = obj.tableName;
                else tableName = obj.name;

                that.connection.query("CREATE TABLE ?? " + queueTableConfig,
                    [tableName], function(err, result){
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

                var sql = "INSERT INTO " + that.table + " (`name`, `password`" +
                    (hasDesc ? ", `description`" : "") +
                    ") VALUES (?, ?" +
                    (hasDesc ? ", ?" : "") + ") ";

                var inserts = [obj.name, obj.passwordHash];
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
        )
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

ConfigDB.prototype.updatePasswordHash = function(name, hash){
    var that = this;
    return updateArg(name, "password", hash, that);
};

ConfigDB.prototype.updateQueueName = function(oldName, newName){
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

ConfigDB.prototype.updateDescription = function(name, desc){
    var that = this;
    return updateArg(name, "description", desc, that);
};

ConfigDB.prototype.deleteQueue = function(name, tableName){
    var that = this;
    var deleteDefer = new Deferred();
    var defer = new Deferred();

    tableName = (tableName === undefined ? name : tableName);
    that.connection.query("DROP TABLE ??", [tableName], function(err, result){
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
    )

    return defer;
};


module.exports = ConfigDB;