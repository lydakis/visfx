d3.queue()
  .defer(d3.csv, 'mock-data/transactions.csv')
  .await(makeGraphs);

function makeGraphs(error, transactions) {
  tr = crossfilter(transactions);
  transactionsByProvider = tr.dimension(function(d) { return d["Provider ID"] });
  this.transactionsByProvider = transactionsByProvider;
  createInputRange(transactionsByProvider.group().size())

  var colors = d3.scale.category20b();

  var color = function(d) { return colors(d["Provider ID"]); };

  this.parcoords = d3.parcoords()("#parcoords-overview")
    .data(transactions)
    .hideAxis(["Date Open", "Date Closed"])
    .color(color)
    .height(500)
    .alpha(0.25)
    .composite("darken")
    .margin({ top: 24, left: 150, bottom: 12, right: 0 })
    .mode("queue")
    .render()
    .brushMode("1D-axes")  // enable brushing
    .reorderable();

  this.parcoords.svg.selectAll("text")
    .style("font", "10px sans-serif");
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

function highlightProvider(provider) {
  if(provider) {
    this.parcoords.unhighlight();
    this.parcoords.highlight(this.transactionsByProvider.filter(provider).top(Infinity));
  }
  else {
    this.parcoords.unhighlight();
  }
}

function createInputRange(maxRange) {
  var providerRange = document.createElement("INPUT");
  providerRange.setAttribute("type", "range");
  providerRange.setAttribute("id", "provider-range");
  providerRange.setAttribute("value", 0);
  providerRange.setAttribute("max", maxRange);
  providerRange.setAttribute("oninput", "rangeChange()");
  document.getElementById("provider-range-div")
    .appendChild(providerRange);
}
