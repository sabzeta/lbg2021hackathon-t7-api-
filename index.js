const {BigQuery} = require('@google-cloud/bigquery');

const express = require('express')
const app = express()
const port = 3000



app.get('/', (req, res) => {
  res.status(200).send('Hello, hackathon!').end();
})

app.get('/covid', (req, res) => {
  return getCovidStats(req, res);
})

app.get('/hotels', (req, res) => {
  return getHotels(req, res);
});

app.get('/places', (req, res) => {
  return getPlacesList(req, res);
});

app.get('/sentiment', (req, res) => {
  return getSentiment(req, res);
});

// Start the server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
  console.log('Press Ctrl+C to quit.');
});
// [END gae_flex_quickstart]

module.exports = app;

const getCovidStats = async (req, res) => {
  const bigqueryClient = new BigQuery();
  const place = req.query.place || 'bristol';
  
  const sqlQuery = `SELECT * FROM \`lbghack2021team7.stage.stage_ukgov_covid19\` 
    where LOWER(Area_name) like '%${place}%'
    order by Specimen_date desc
    LIMIT 1`;

  const options = {
    query: sqlQuery,
  };

  // Run the query
  const [rows] = await bigqueryClient.query(options);
    
  res.set('Access-Control-Allow-Origin', "*")
  res.set('Access-Control-Allow-Methods', 'GET')
  res.status(200).send(JSON.stringify(rows));
};

const getHotels = async (req, res) => {
  const bigqueryClient = new BigQuery();
  const place = req.query.place || 'bristol';
  
  const sqlQuery = `SELECT * FROM \`lbghack2021team7.stage.stage_hotels\`
    where LOWER(Location) like '%${place}%'
    order by Rating desc 
    LIMIT 20`;

  const options = {
    query: sqlQuery,
  };

  // Run the query
  const [rows] = await bigqueryClient.query(options);
    
  res.set('Access-Control-Allow-Origin', "*")
  res.set('Access-Control-Allow-Methods', 'GET')
  res.status(200).send(JSON.stringify(rows));
};

const getPlacesList = async (req, res) => {
  const bigqueryClient = new BigQuery();
  const sqlQuery=`SELECT Area_name, Area_type, AVG(Daily_lab_confirmed_cases) as cases 
  FROM \`lbghack2021team7.stage.stage_ukgov_covid19\` 
  Group by Area_name, Area_type
  HAVING Area_type = 'ltla'
  order by cases
  LIMIT 10`;
  const options = {
    query: sqlQuery,
  };

  // Run the query
  const [rows] = await bigqueryClient.query(options);

  res.set('Access-Control-Allow-Origin', "*")
  res.set('Access-Control-Allow-Methods', 'GET')
  res.status(200).send(JSON.stringify(rows));
};

const getSentiment = async (req, res) => {
  const bigqueryClient = new BigQuery();
  const place = req.query.place || 'bristol';
  
  const sqlQuery=`SELECT AVG(SentimentScore) FROM \`lbghack2021team7.stage.stage_sentiment\` 
  where LOWER(Tweet) like '%${place}%'`;

  const options = {
    query: sqlQuery,
  };

  // Run the query
  const [rows] = await bigqueryClient.query(options);
      
  res.set('Access-Control-Allow-Origin', "*")
  res.set('Access-Control-Allow-Methods', 'GET')
  res.status(200).send(JSON.stringify(rows));
};
