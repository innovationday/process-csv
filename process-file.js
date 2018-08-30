/* File Processing, Reading csv file, convert to JSON Array and storing in MongodB*/
var tStart = new Date().getTime();

var dbName="process-csv";
var dbCollection="SUPER_HEROS";
var fs = require('fs');

var parse = require('csv-parse');
 
var inputFile='marvel-data.csv';
console.log("Processing  file : "+inputFile);
var arr=new Array();
var parser = parse({delimiter: ','}, function (err, data) {
    // when all data is available,then process them
    // note: array element at index 0 contains the row of headers that we should skip
    data.forEach(function(line) {
      // create jsonObj object out of parsed fields
        var jsonObj = {"name" : line[1] ,
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

     //console.log(JSON.stringify(jsonObj));
    
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
        console.log("No of json documents inserted in MongoDB  : "+res.insertedCount);
        console.log("Time taken by the process in milliseconds : " + (tEnd - tStart));
        db.close();
      });
    });
   
   
}