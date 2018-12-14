const fs             = require('fs');
const mysql          = require('mysql');
const wkt            = require('terraformer-wkt-parser');
const { Transform }  = require('stream');
const csvWriter      = require('csv-write-stream')

const connection = mysql.createConnection({
  host     : '',
  user     : '',
  password : '',
  database : '',
  ssl  : {
    ca : fs.readFileSync(__dirname + '/cert.crt.pem')
  }
});

const fileWriterStream = fs.createWriteStream(__dirname + '/output.csv');

const csvWriteStream = csvWriter();

const transformStream = new Transform({
  objectMode: true,
  transform(chunk, enc, cb) {
    row = { ...chunk };
    let primitive = wkt.parse(chunk.wkt);
    row.lng = primitive.coordinates[0];
    row.lat = primitive.coordinates[1];
    delete row.wkt;
    row.language = chunk.language ? chunk.language : "";
    row.content_text = chunk.content_text ? chunk.content_text.replace(/[\n\,\"\']/g,"") : "";
    this.push(row);
    cb();
  }
});
transformStream.on('finish', () => {
  connection.end();
});

connection.connect();

connection.query(`
  select 
    fields
  from 
    tables`
)
.stream({highWaterMark: 5})
.pipe(transformStream)
.pipe(csvWriteStream)
.pipe(fileWriterStream);