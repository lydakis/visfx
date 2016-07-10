d3.queue()
  .defer(d3.csv, "mock-data/transactions.csv")
  .await(makeGraphs);

var advancedFiltering = false;

var colors = d3.scale.category20b();
var color = function(d) { return colors(d.provider_id); };

var parcoords = d3.parcoords()("#parcoords-overview")
  .color(color)
  .height(750)
  .alpha(0.25)
  .composite("darken");

var ppaChart = dc.barChart("#ppa")
  .width(600)
  .height(400)

var currencyCounts = dc.pieChart("#currency-counts")
  .width(400)
  .height(400)
  .radius(180)

function makeGraphs(error, transactions) {
  formatData(transactions);

  this.tr = crossfilter(transactions);
  this.transactionsByProvider = tr.dimension(function(d) {
    return d.provider_id;
  });
  setupInputRange()

  var transactionsByID = tr.dimension(function(d) {
    return d.transaction_id;
  })

  var transactionsByDateClosed = tr.dimension(function(d) {
    return d3.time.format("%Y-%m-%d").parse(
      d.date_closed.getFullYear() + "-" +
      (+d.date_closed.getMonth() + 1) + "-" +
      d.date_closed.getDate());
  });

  var transactionsByDateClosedGroup = transactionsByDateClosed
    .group()
    .reduce(
      function(p, v) {
        p.amount += +v.amount;
        p.pnl += +v.net_pnl;
        p.ppa = (p.amount !== 0) ? p.pnl / p.amount : 0;
        return p;
      },
      function(p, v) {
        p.amount -= +v.amount;
        p.pnl -= +v.net_pnl;
        p.ppa = (p.amount !== 0) ? p.pnl / p.amount : 0;
        return p;
      },
      function() {
        return { amount: 0, pnl: 0, ppa: 0 };
      }
    );

  var transactionsByCurrency = tr.dimension(function(d) {
    return d.currency_pair;
  });

  var transactionsByCurrencyGroup = transactionsByCurrency.group();

  var parcoordsDimensions = {
    "provider_id": { index: 0, title: "Provider ID" },
    "transaction_type": { index: 1, title: "Transation Type" },
    "currency_pair": { index: 2, title: "Currency Pair" },
    "country": { index: 3, title: "Country" },
    "date_open": { index: 4, title: "Date Open" },
    "date_closed": { index: 5, title: "Date Closed" },
    "amount": { index: 6, title: "Amount (lots)" },
    "net_pnl": { index: 7, title: "Net PnL ($)" },
  };

  parcoords
    .data(transactions)
    .dimensions(parcoordsDimensions)
    .margin({ top: 24, left: 150, bottom: 12, right: 0 })
    .mode("queue")
    .render()
    .hideAxis(["transaction_id"])
    .brushMode("1D-axes-multi")
    .reorderable();

  parcoords.on("brush", function(d) {
    transactionsByID.filterAll();
    dc.redrawAll();
    d3.queue().defer(filterBrushed, d)
      .await(function(error) {
        if (error) throw error;
        dc.redrawAll();
      })
  })

  function filterBrushed(brushed, callback) {
    if (advancedFiltering && brushed.length !== transactions.length) {
      transactionsByID.filter(function(d) {
        return brushed.map(function(d) {
          return d.transaction_id;
        }).indexOf(d) + 1;
      });
    }
    callback(null);
  }

  ppaChart
    .margins({ top: 24, left: 60, bottom: 20, right: 0 })
    .dimension(transactionsByDateClosed)
    .group(transactionsByDateClosedGroup)
    .valueAccessor(function(d) { return +d.value.ppa; })
    .x(d3.time.scale().domain([new Date(2015, 3, 30), new Date(2015, 4, 8)]))
    .renderHorizontalGridLines(true)
    .brushOn(true)
    .clipPadding(10)
    .elasticY(true)
    .centerBar(true)
    .gap(10)
    .xUnits(function(){return 10;})
    .round(dc.round.floor)
    .alwaysUseRounding(true)
    .yAxisLabel('Net PNL per Amount');

  currencyCounts
    .dimension(transactionsByCurrency)
    .group(transactionsByCurrencyGroup);

  dc.renderAll();
}

function formatData(data) {
  var dateFormat = d3.time.format("%Y-%m-%dT%H:%M:%S.%LZ");

  data.forEach(function(d) {
    d.transaction_id = +d.transaction_id;
    d.provider_id = +d.provider_id;
    d.amount = +d.amount;
    d.net_pnl = +d.net_pnl;
    d.date_open = dateFormat.parse(d.date_open);
    d.date_closed = dateFormat.parse(d.date_closed);
  })
}

function rangeChanged() {
  var providerRange = document.getElementById("provider-range");
  var value = document.getElementById("provider-value");
  if ("0" !== providerRange.value) {
    provider = this.transactionsByProvider.group().orderNatural().all()[providerRange.value - 1].key;
    value.innerHTML = provider;
    highlightProvider(provider);
  }
  else {
    value.innerHTML = "All";
    highlightProvider(null);
  }
}

function toggleCheckbox() {
  var checkbox = document.getElementById("checkbox");
  if (true === checkbox.checked) {
    advancedFiltering = true;
  }
  else {
    advancedFiltering = false;
  }
}

function redrawCharts() {
  dc.redrawAll();
}

function dateChanged() {
  var dateFormat = d3.time.format("%Y-%m-%d");
  var startDate = document.getElementById("start-date").value;
  var endDate = document.getElementById("end-date").value;
  if (validateDate(startDate) && validateDate(endDate)) {
    parcoords.data([]).render();
    this.tr.remove();
    dc.redrawAll();
    d3.queue()
      .defer(d3.json,
        "http://83.212.100.48:5000/transactions?start_date=" + startDate + "&end_date=" + endDate)
      .await(function(error, data) {
        if (error) throw error;
        formatData(data);
        parcoords.data(data).render();
        this.tr.add(data);
        setupInputRange();
        ppaChart
          .x(d3.time.scale().domain(
            [d3.time.day.offset(dateFormat.parse(startDate), -1),
            d3.time.day.offset(dateFormat.parse(endDate), 1)]));
        dc.redrawAll();
      });
  }
}

function validateDate(date) {
  if (null !== d3.time.format("%Y-%m-%d").parse(date)) {
    return true;
  }
  return false;
}

function highlightProvider(provider) {
  if (provider) {
    this.parcoords.highlight(this.transactionsByProvider.filter(provider).top(Infinity));
  }
  else {
    this.parcoords.unhighlight();
    this.transactionsByProvider.filterAll();
  }
}

function setupInputRange() {
  document.getElementById("provider-range")
    .setAttribute("max", this.transactionsByProvider.group().size());
}
