var ES_HOST = {
    host: "83.212.100.48:9200"
};

var client = elasticsearch.Client(ES_HOST);

var getTransactions = function(startDate, endDate, callback) {
  var docs = [];
  var query = {
    query: {
      match_all: { }
    },
    filter: {
      range: {
        date_closed: {
          gte: startDate,
          lte: endDate,
          format: "yyyy-MM-dd"
        }
      }
    }
  };

  client.search({
    index: "forex",
    type: "transaction",
    size: 2000,
    scroll: "2m",
    search_type: "scan",
    body: query
  }, function getMoreUntilDone(error, response) {
    response.hits.hits.forEach(function (hit) {
      delete hit._source["language"];
      delete hit._source["type"];
      delete hit._source["@timestamp"];
      delete hit._source["@version"];
      docs.push(hit._source);
    });
    if (response.hits.total !== docs.length) {
      client.scroll({
        scrollId: response._scroll_id,
        scroll: '2m'
      }, getMoreUntilDone);
    }
    else {
      callback(docs, startDate, endDate)
    }
  });
}
