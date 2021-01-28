const {BigQuery} = require('@google-cloud/bigquery');

const express = require('express')
const app = express()
const port = 3000

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
  const sqlQuery="SELECT * FROM `bigquery-public-data.covid19_govt_response.oxford_policy_tracker` LIMIT 1000";
  const options = {
    query: sqlQuery,
  };

  // Run the query
  // const [rows] = await bigqueryClient.query(options);

  const rows = [
        "Bristol",
        "London",
        "Truro",
        "St Ives",
        "Bournemouth"
    ];
    
  res.set('Access-Control-Allow-Origin', "*")
  res.set('Access-Control-Allow-Methods', 'GET')
  res.status(200).send(JSON.stringify(rows));
};

const getSentiment = async (req, res) => {
  const bigqueryClient = new BigQuery();
  const sqlQuery="SELECT * FROM `bigquery-public-data.covid19_govt_response.oxford_policy_tracker` LIMIT 1000";
  const options = {
    query: sqlQuery,
  };

  // Run the query
  // const [rows] = await bigqueryClient.query(options);

  const rows =  42;
      
  res.set('Access-Control-Allow-Origin', "*")
  res.set('Access-Control-Allow-Methods', 'GET')
  res.status(200).send(JSON.stringify(rows));
};
