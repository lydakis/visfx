d3.queue()
  .defer(d3.csv, "mock-data/transactions.csv")
  .await(makeGraphs);

var colors = d3.scale.category20b();
var color = function(d) { return colors(d.country); };

var parcoords = d3.parcoords()("#parcoords-overview")
  .color(color)
  .height(750)
  .alpha(0.25)
  .composite("darken");

var ppaChart = dc.lineChart("#ppa")
  .width(600)
  .height(400);

var currencyCounts = dc.pieChart("#currency-counts")
  .width(400)
  .height(400)
  .radius(180);

var tradingVolume = dc.lineChart("#trading-volume")
  .width(600)
  .height(400);

var pairPNL = dc.barChart("#pair-pnl")
  .width(1120)
  .height(400);

// var providerPNL = dc.barChart("#provider-pnl")
//   .width(1120)
//   .height(400);

var totalProfits = dc.numberDisplay("#total-profits");
var totalLosses = dc.numberDisplay("#total-losses");
var netPNL = dc.numberDisplay("#net-pnl");
var dataTable = dc.dataTable("#data-table");

function makeGraphs(error, transactions) {
  formatData(transactions);

  this.tr = crossfilter(transactions);
  this.transactionsByProvider = tr.dimension(function(d) {
    return d.provider_id;
  });
  var transactionsByProviderPNLGroup = transactionsByProvider.group().reduce(
    function(p, v) {
      p.net_pnl += v.net_pnl;
      return p;
    },
    function(p, v) {
      p.net_pnl -= v.net_pnl;
      return p;
    },
    function() {
      return { net_pnl: 0 };
    }
  );

  var transactionsByID = tr.dimension(function(d) {
    return d.transaction_id;
  });

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
  var transactionsByCurrency2 = tr.dimension(function(d) {
    return d.currency_pair;
  });
  transactionsByCurrency3 = tr.dimension(function(d) {
    return d.currency_pair;
  });
  var transactionsByCurrencyPNLGroup = transactionsByCurrency2.group().reduce(
    function(p, v) {
      p.net_pnl += v.net_pnl;
      return p;
    },
    function(p, v) {
      p.net_pnl -= v.net_pnl;
      return p;
    },
    function() {
      return { net_pnl: 0 };
    }
  );

  var transactionsByDateClosedHourly = tr.dimension(function(d) {
    return d3.time.format("%Y-%m-%dT%H").parse(
      d.date_closed.getFullYear() + "-" +
      (+d.date_closed.getMonth() + 1) + "-" +
      d.date_closed.getDate() + "T" +
      d.date_closed.getHours());
  });
  var transactionsByDateClosedHourlyGroup = transactionsByDateClosedHourly.group();
  var transactionsByDateClosedHourlyPNLGroup = transactionsByDateClosedHourly.group().reduce(
    function(p, v) {
      p.net_pnl += v.net_pnl;
      return p;
    },
    function(p, v) {
      p.net_pnl -= v.net_pnl;
      return p;
    },
    function() {
      return { net_pnl: 0 };
    }
  );

  transactionsByPNLGroup = tr.dimension(function(d) {
    return d.transaction_id;
  })
    .groupAll().reduce(
      function(p, v) {
        p.p += v.net_pnl >= 0 ? v.net_pnl : 0;
        p.l += v.net_pnl < 0 ? v.net_pnl : 0;
        p.net_pnl += v.net_pnl;
        return p;
      },
      function(p, v) {
        p.p -= v.net_pnl >= 0 ? v.net_pnl : 0;
        p.l -= v.net_pnl < 0 ? v.net_pnl : 0;
        p.net_pnl -= v.net_pnl;
        return p;
      },
      function() {
        return { p: 0, l: 0, net_pnl: 0 };
      }
    );

  transactionsByType = tr.dimension(function(d) {
    return d.transaction_type;
  });
  transactionsByCountry = tr.dimension(function(d) {
    return d.country;
  });

  var parcoordsDimensions = generateParcoordsDimensions();

  parcoords
    .data(transactions)
    .dimension(transactionsByID)
    .dimensions(parcoordsDimensions)
    .margin({ top: 24, left: 150, bottom: 12, right: 0 })
    .mode("queue")
    .render()
    .hideAxis(["transaction_id"])
    .brushMode("1D-axes-multi")
    .reorderable();

  ppaChart
    .margins({ top: 24, left: 60, bottom: 20, right: 0 })
    .dimension(transactionsByDateClosedHourly)
    .group(transactionsByDateClosedHourlyPNLGroup)
    // .interpolate("bundle")
    .valueAccessor(function(d) { return +d.value.net_pnl; })
    .x(d3.time.scale().domain([new Date(2015, 4, 1), new Date(2015, 4, 7)]))
    .renderHorizontalGridLines(true)
    .renderVerticalGridLines(true)
    .brushOn(true)
    .clipPadding(10)
    .elasticY(true)
    // .gap(10)
    .xUnits(d3.time.days)
    .yAxisLabel("Net PNL ($)");

  pairPNL
    .margins({ top: 24, left: 60, bottom: 45, right: 0 })
    .dimension(transactionsByCurrency2)
    .group(transactionsByCurrencyPNLGroup)
    .valueAccessor(function(d) { return +d.value.net_pnl; })
    .x(d3.scale.ordinal().domain([""]))
    .renderHorizontalGridLines(true)
    .renderVerticalGridLines(true)
    .brushOn(true)
    .clipPadding(10)
    .elasticY(true)
    .elasticX(true)
    .xUnits(dc.units.ordinal)
    .yAxisLabel("Net PNL ($)");

  // providerPNL
  //   .margins({ top: 24, left: 60, bottom: 45, right: 0 })
  //   .dimension(transactionsByProvider)
  //   .group(transactionsByProviderPNLGroup)
  //   .valueAccessor(function(d) { return +d.value.net_pnl; })
  //   .x(d3.scale.linear().domain([0, 2]))
  //   .renderHorizontalGridLines(true)
  //   .renderVerticalGridLines(true)
  //   .brushOn(true)
  //   .clipPadding(10)
  //   .elasticY(true)
  //   .elasticX(true)
  //   // .xUnits(dc.units.ordinal)
  //   .yAxisLabel("Net PNL ($)");

  currencyCounts
    .dimension(transactionsByCurrency)
    .group(transactionsByCurrencyGroup);

  tradingVolume
    .margins({ top: 24, left: 60, bottom: 20, right: 0 })
    .x(d3.time.scale().domain([new Date(2015, 4, 1), new Date(2015, 4, 7)]))
    // .interpolate("bundle")
    .brushOn(true)
    .renderHorizontalGridLines(true)
    .renderVerticalGridLines(true)
    .yAxisLabel("Trading Volume")
    .clipPadding(10)
    .elasticY(true)
    .dimension(transactionsByDateClosedHourly)
    .group(transactionsByDateClosedHourlyGroup);

  totalProfits
    .formatNumber(function(d) { return "$ " + d3.format(",.2f")(d); })
    .valueAccessor(function(d) { return d.p; })
    .group(transactionsByPNLGroup);

  totalLosses
    .formatNumber(function(d) { return "$ " + d3.format(",.2f")(d); })
    .valueAccessor(function(d) { return d.l; })
    .group(transactionsByPNLGroup);

  netPNL
    .formatNumber(function(d) { return "$ " + d3.format(",.2f")(d); })
    .valueAccessor(function(d) { return d.net_pnl; })
    .group(transactionsByPNLGroup);

  dataTable
    .dimension(transactionsByID)
    .group(function(d) {
      return "Number of Trades: " + transactionsByProvider.top(Infinity).length;
    })
    .columns([
      {
        label: "Provider ID",
        format: function(d) { return d.provider_id; }
      },
      {
        label: "Country",
        format: function(d) { return d.country; }
      },
      {
        label: "Transation Type",
        format: function(d) { return d.transaction_type; }
      },
      {
        label: "Currecy Pair",
        format: function(d) { return d.currency_pair; }
      },
      {
        label: "Date Open",
        format: function(d) { return d.date_open; }
      },
      {
        label: "Date Closed",
        format: function(d) { return d.date_closed; }
      },
      {
        label: "Amount (lots)",
        format: function(d) { return d3.format(",")(d.amount); }
      },
      {
        label: "Net PnL ($)",
        format: function(d) { return d3.format(",")(d.net_pnl); }
      },
    ])
    .size(Infinity)
    .sortBy(function(d) { return d.net_pnl; })
    .order(d3.descending);
  update();

  setupInputRange();
  updateSelections(true);
  dc.renderAll();
}

var ofs = 0, pag = 17;
function display() {
  d3.select('#begin')
    .text(ofs);
  d3.select('#end')
    .text(ofs+pag-1);
  d3.select('#last')
    .attr('disabled', ofs-pag<0 ? 'true' : null);
  d3.select('#next')
    .attr('disabled', ofs+pag>=transactionsByProvider.top(Infinity).length ? 'true' : null);
  d3.select('#size').text(transactionsByProvider.top(Infinity).length);
}
function update() {
  dataTable.beginSlice(ofs);
  dataTable.endSlice(ofs+pag);
  display();
  }
function next() {
  ofs += pag;
  update();
  dataTable.redraw();
}
function last() {
  ofs -= pag;
  update();
  dataTable.redraw();
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
  });
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
    parcoords.advancedFiltering(true);
  }
  else {
    parcoords.advancedFiltering(false);
  }
}

function redrawCharts() {
  dc.redrawAll();
}

function dateChanged() {
  var startDate = document.getElementById("start-date").value;
  var endDate = document.getElementById("end-date").value;
  if (validateDate(startDate) && validateDate(endDate)) {
    parcoords
      .dimensions({})
      .data([])
      .render();
    this.tr.remove();
    dc.redrawAll();
    getTransactions(startDate, endDate, updateData);
  }
}

function updateData(data, startDate, endDate) {
  var dateFormat = d3.time.format("%Y-%m-%d");
  formatData(data);
  parcoords
    .data(data)
    .dimensions(generateParcoordsDimensions())
    .render()
    .updateAxes();
  this.tr.add(data);
  setupInputRange();
  ppaChart
    .x(d3.time.scale().domain(
      [dateFormat.parse(startDate), dateFormat.parse(endDate)]));
  tradingVolume
    .x(d3.time.scale().domain(
      [dateFormat.parse(startDate), dateFormat.parse(endDate)]));
  dc.redrawAll();
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

function generateParcoordsDimensions() {
  return {
    "provider_id": { index: 0, title: "Provider ID" },
    "country": { index: 1, title: "Country" },
    "transaction_type": { index: 2, title: "Transation Type" },
    "currency_pair": { index: 3, title: "Currency Pair" },
    "date_open": { index: 4, title: "Date Open" },
    "date_closed": { index: 5, title: "Date Closed" },
    "amount": { index: 6, title: "Amount (lots)" },
    "net_pnl": { index: 7, title: "Net PnL ($)" },
  };
}

function setupInputRange() {
  document.getElementById("provider-range")
    .setAttribute("max", this.transactionsByProvider.group().size());
}

function changeSelection(selection) {
  var providerSelection = document.getElementById("provider-selection");
  var countrySelection = document.getElementById("country-selection");
  var typeSelection = document.getElementById("type-selection");
  var currencySelection = document.getElementById("currency-selection");
  if (providerSelection.value !== "All") {
    this.parcoords.highlight(this.transactionsByProvider.filter(providerSelection.value).top(Infinity));
  }
  else {
    this.transactionsByProvider.filterAll();
  }
  if (countrySelection.value !== "All") {
    this.parcoords.highlight(this.transactionsByCountry.filter(countrySelection.value).top(Infinity));
  }
  else {
    this.transactionsByCountry.filterAll();
  }
  if (typeSelection.value !== "All") {
    this.parcoords.highlight(this.transactionsByType.filter(typeSelection.value).top(Infinity));
  }
  else {
    this.transactionsByType.filterAll();
  }
  if (currencySelection.value !== "All") {
    this.parcoords.highlight(this.transactionsByCurrency3.filter(currencySelection.value).top(Infinity));
  }
  else {
    this.transactionsByCurrency3.filterAll();
  }
  if (providerSelection.value === "All" &&
      countrySelection.value === "All" &&
      typeSelection.value === "All" &&
      currencySelection.value === "All") {
    this.parcoords.unhighlight();
  }

  dc.redrawAll();
}

function updateSelections(first) {
  var providerSelection = document.getElementById("provider-selection");
  var countrySelection = document.getElementById("country-selection");
  var typeSelection = document.getElementById("type-selection");
  var currencySelection = document.getElementById("currency-selection");

  providerSelection.multiple = true;

  if (first) {
    var pAll = document.createElement("option");
    pAll.text = "All";
    pAll.selected = "selected";
    providerSelection.add(pAll);
    var lAll = document.createElement("option");
    lAll.text = "All";
    countrySelection.add(lAll);
    var tAll = document.createElement("option");
    tAll.text = "All";
    typeSelection.add(tAll);
    var cAll = document.createElement("option");
    cAll.text = "All";
    currencySelection.add(cAll);
  }
  this.transactionsByProvider.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    providerSelection.add(option);
  });
  this.transactionsByCountry.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    countrySelection.add(option);
  });
  this.transactionsByType.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    typeSelection.add(option);
  });
  this.transactionsByCurrency3.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    currencySelection.add(option);
  });
}
