d3.queue()
  .defer(d3.csv, 'mock-data/transactions.csv')
  .await(makeGraphs);

function makeGraphs(error, transactions) {
  var dateFormat = d3.time.format("%Y-%m-%dT%H:%M:%S.%LZ");

  this.transactions = transactions;

  transactions.forEach(function(d) {
    d["Provider ID"] = +d["Provider ID"];
    d["Amount (lots)"] = +d["Amount (lots)"];
    d["Net PNL ($)"] = +d["Net PNL ($)"];
    d["Date Open"] = dateFormat.parse(d["Date Open"]);
    d["Date Closed"] = dateFormat.parse(d["Date Closed"]);
  })

  tr = crossfilter(transactions);
  var transactionsByProvider = tr.dimension(function(d) {
    return d["Provider ID"];
  });
  this.transactionsByProvider = transactionsByProvider;
  createInputRange(transactionsByProvider.group().size())

  var transactionsByDateClosed = tr.dimension(function(d) {
    return d3.time.format("%Y-%m-%d").parse(
      d["Date Closed"].getFullYear() + "-" +
      (+d["Date Closed"].getMonth() + 1) + "-" +
      d["Date Closed"].getDate());
  });

  var transactionsByDateClosedGroup = transactionsByDateClosed
    .group()
    .reduce(
      function(p, v) {
        p.amount += +v["Amount (lots)"];
        p.pnl += +v["Net PNL ($)"];
        p.ppa = (p.amount !== 0) ? p.pnl / p.amount : 0;
        return p;
      },
      function(p, v) {
        p.amount -= +v["Amount (lots)"];
        p.pnl -= +v["Net PNL ($)"];
        p.ppa = (p.amount !== 0) ? p.pnl / p.amount : 0;
        return p;
      },
      function() {
        return { amount: 0, pnl: 0, ppa: 0 };
      }
    );

  var transactionsByCurrency = tr.dimension(function(d) {
    return d["Currency"];
  });

  var transactionsByCurrencyGroup = transactionsByCurrency.group();

  var minPPA = d3.min(transactionsByDateClosedGroup.all(), function(d) { return +d.value.ppa; });
  var maxPPA = d3.max(transactionsByDateClosedGroup.all(), function(d) { return +d.value.ppa; })


  var colors = d3.scale.category20b();

  // var colors = d3.scale.linear()
  //   .domain([minPnL, maxPnL])
  //   .range(["#b63032", "#346493"])
  //   .interpolate(d3.interpolateLab);

  var color = function(d) { return colors(d["Provider ID"]); };

  this.parcoords = d3.parcoords()("#parcoords-overview")
    .data(transactions)
    .hideAxis([])
    .color(color)
    .height(750)
    .alpha(0.25)
    .composite("darken")
    .margin({ top: 24, left: 150, bottom: 12, right: 0 })
    .mode("queue")
    .render()
    .brushMode("1D-axes-multi")  // enable brushing
    .reorderable();

  this.parcoords.svg.selectAll("text")
    .style("font", "8px sans-serif");


  this.linechart = dc.barChart("#ppa")
    .width(600)
    .height(400)
    .dimension(transactionsByDateClosed)
    .group(transactionsByDateClosedGroup)
    .valueAccessor(function(d) { return +d.value.ppa; })
    .x(d3.time.scale().domain([new Date(2015, 4, 1), new Date(2015, 4, 8)]))
    .renderHorizontalGridLines(true)
    .brushOn(true)
    .clipPadding(10)
    .elasticY(true)
    .xUnits(function(){return 8;})
    .round(dc.round.floor)
    .alwaysUseRounding(true);

  var piechart = dc.pieChart("#currency-counts")
    .width(400)
    .height(400)
    .radius(180)
    .dimension(transactionsByCurrency)
    .group(transactionsByCurrencyGroup);



  dc.renderAll();
}

function rangeChange() {
  var providerRange = document.getElementById("provider-range");
  var value = document.getElementById("provider-value");
  if ("0" !== providerRange.value) {
    provider = this.transactionsByProvider.group().orderNatural().all()[providerRange.value - 1].key;
    value.innerHTML = provider;
    highlightProvider(provider)
  }
  else {
    value.innerHTML = "All"
    highlightProvider(null)
  }
}

function redrawCharts() {
  dc.redrawAll();
}

function highlightProvider(provider) {
  if(provider) {
    this.parcoords.highlight(this.transactionsByProvider.filter(provider).top(Infinity));
  }
  else {
    this.parcoords.unhighlight();
    this.transactionsByProvider.filterAll();
  }
}

function createInputRange(maxRange) {
  var providerRange = document.createElement("INPUT");
  providerRange.setAttribute("type", "range");
  providerRange.setAttribute("id", "provider-range");
  providerRange.setAttribute("value", 0);
  providerRange.setAttribute("max", maxRange);
  providerRange.setAttribute("oninput", "rangeChange()");
  providerRange.setAttribute("onchange", "redrawCharts()");
  document.getElementById("provider-range-div")
    .appendChild(providerRange);
}
