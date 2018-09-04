/* File Processing, Reading csv file, convert to JSON Array and storing in MongodB*/
var tStart = new Date().getTime();

var dbName="process-csv";
var dbCollection="SUPER_HEROS";
var fs = require('fs');

var parse = require('csv-parse');

//var inputFile='marvel-data.csv';
var inputFile='GetTrades.csv';
console.log("Processing  file : "+inputFile);
var arr=new Array();
var parser = parse({delimiter: ','}, function (err, data) {
    // when all data is available,then process them
    // note: array element at index 0 contains the row of headers that we should skip
    data.forEach(function(line) {
      // create jsonObj object out of parsed fields
      /*  var jsonObj = {"name" : line[1] ,
        "urlslug" : line[2] ,
        "ID" : line[3] ,
        "ALIGN" : line[4] ,
        "EYE" : line[5] ,
        "HAIR" : line[6] ,
        "SEX" : line[7] ,
        "GSM" : line[8] ,
        "ALIVE" : line[9] ,
        "APPEARANCES" : line[10] ,
        "FIRST APPEARANCE" : line[11] ,
        "Year" : line[12]
                    };
*/

 var jsonObj = {"Symbol" : line[0] ,
 "StartTime" : line[1] ,
        "EndTime" : line[2] ,
        "Trades Timestamp" : line[3] ,
        "Trades Quantity" : line[4] ,
        "Trades Price" : line[5] ,
        "Trades Exchange" : line[6] ,
        "Trades MarketCenter" : line[7] ,
        "Trades SubMarketCenter" : line[8] ,
        "Trades TRDI" : line[9] ,
        "Trades CanceledIndicator" : line[10] ,
        "Trades DOTT" : line[11] ,
        "Trades ITY" : line[12],
        "Trades MSN" : line[13],
        "Trades OMSN" : line[14],
        "Outcome" : line[15],
        "Message" : line[16],
        "Delay" : line[17]
                    };

    // console.log(JSON.stringify(jsonObj));

    arr.push(jsonObj);
    });
    console.log("No of documents retrieved from csv file : "+arr.length);
    storeDataInDB(arr);
});

// read the inputFile, feed the contents to the parser
fs.createReadStream(inputFile).pipe(parser);


function storeDataInDB(arr1){

    var MongoClient = require('mongodb').MongoClient;
    var url = "mongodb://127.0.0.1:27017/";

    MongoClient.connect(url, {useNewUrlParser: true}, function(err, db) {
        useNewUrlParser: true
      if (err) throw err;
      var dbo = db.db(dbName);
      dbo.collection(dbCollection).insertMany(arr, function(err, res) {
        if (err) throw err;
        var tEnd = new Date().getTime();
        var timeTaken = tEnd - tStart;


        console.log("No of json documents inserted in MongoDB  : "+res.insertedCount);
        if(timeTaken<1000){
		console.log("Time taken by the process : " + timeTaken+"milli seconds");
		}else{
		console.log("Time taken by the process : " + timeTaken/1000+" seconds");
		}
        db.close();
      });
    });


}