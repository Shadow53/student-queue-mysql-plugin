/**
 * Created by michael on 3/8/16.
 */
var mysql = require("./mysql.js");

var db = new mysql.RequestDB({
    host: "localhost",
    user: "studentqueue",
    password: "password",
    database: "studentqueue",
    table: "default"
});

/*db.createTable("default").then(
    function(){
        db.resetTable("default").then(
            function(){
                db.add({id: "111111", name: "Michael", problem: "Just testing"}).then(
                    function(){
                        db.remove("111111").then(
                            function(){
                                console.log("Success!");
                            },
                            function(err){ console.log(err) }
                        );
                    },
                    function(err){ console.log(err) }
                );
            },
            function(err){ console.log(err) }
        );
    },
    function(err){ console.log(err) }
);*/

var config = new mysql.ConfigDB({
    host: "localhost",
    user: "studentqueue",
    password: "password",
    database: "studentqueue",
    table: "config"
});

config.createConfigTable().then(
    function(){
        config.addNewQueue({
            name: "Test",
            passwordHash: "TESTIGN",
            description: "ESPGNVNDFKVDP EPRVME RPVM E PEVM EPR"
        }).then(
            function(){
                console.log("added");
                setTimeout(function(){
                    config.deleteQueue("Test").then(
                        function(){
                            console.log("success");
                        },
                        function(err){console.log(err)}
                    )
                }, 10000)
            },
            function(err){console.log(err)}
        )
    },
    function(err){console.log(err)}
);