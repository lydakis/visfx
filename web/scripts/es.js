var ES_HOST = {
    host: "83.212.100.48:9200"
};

var client = elasticsearch.Client(ES_HOST);

function getTransactions(startDate, endDate, callback) {
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
    response.hits.hits.forEach(function(hit) {
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
      callback(docs, startDate, endDate);
    }
  });
}

function getRates(currencyPair, startDate, endDate, callback) {
  var docs = [];
  var query = {
    size: 0,
    aggs: {
      range: {
        date_range: {
          field: "tick_date",
          format: "yyyy-MM-dd",
          ranges: [
            { from: startDate, to: endDate },
          ]
        },
        aggs: {
          resolution: {
            date_histogram: {
              field: "tick_date",
              interval: "30m"
            },
            aggs: {
              avg_bid: {
                avg: {
                  field: "bid_price"
                }
              },
              avg_ask: {
                avg: {
                  field: "ask_price"
                }
              }
            }
          }
        }
      }
    }
  };

  client.search({
    index: "forex",
    type: "history",
    body: query
  }, function getMoreUntilDone(error, response) {
    response.aggregations.range.buckets[0].resolution.buckets.forEach(function(d) {
      var dateFormat = d3.time.format("%Y-%m-%dT%H:%M:%S.%LZ");
      if (d.avg_ask.value !== null || d.avg_bid.value !== null) {
        docs.push({ key: dateFormat.parse(d.key_as_string), isBid: 1, value: d.avg_bid.value })
        docs.push({ key: dateFormat.parse(d.key_as_string), isBid: 0, value: d.avg_ask.value })
      }
    });
    callback(docs, currencyPair, startDate, endDate)
  });
}
