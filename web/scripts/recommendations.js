d3.queue()
  .defer(d3.csv, "mock-data/recommendations.csv")
  .defer(d3.csv, "mock-data/ratings.csv")
  .defer(d3.csv, "mock-data/fratings.csv")
  .await(makeGraphs);

var colors = d3.scale.ordinal()
  .domain(["NO", "YES"])
  .range(["steelblue", "brown"]);
var color = function(d) { return colors(d.isRecommendation); };

var parcoords = d3.parcoords()("#parcoords-overview")
  .color(color)
  .height(750)
  .alpha(0.25)
  .composite("darken");

var previousTrades = dc.dataTable("#data-table-prev");
var recommendationsTable = dc.dataTable("#data-table-rec");
var futureTrades = dc.dataTable("#data-table-future");

function makeGraphs(error, recommendations, ratings, fratings) {
  formatData(recommendations);
  formatData(ratings);
  formatData(fratings);

  this.data = ratings.concat(recommendations);

  rc = crossfilter(recommendations);
  rt = crossfilter(ratings);
  dt = crossfilter(data);
  fr = crossfilter(fratings);

  dataByProvider = dt.dimension(function(d) {
    return d.provider_id;
  })
  recommendationsByProvider = rc.dimension(function(d) {
    return d.provider_id;
  })
  recommendationsByProvider.filter(0);
  ratingsByProvider = rt.dimension(function(d) {
    return d.provider_id;
  })
  ratingsByProvider.filter(0);
  fratingsByProvider = fr.dimension(function(d) {
    return d.provider_id;
  })
  fratingsByProvider.filter(0);

  var parcoordsDimensions = generateParcoordsDimensions();

  parcoords
    .data(data)
    .dimensions(parcoordsDimensions)
    .margin({ top: 24, left: 150, bottom: 12, right: 0 })
    .mode("queue")
    .render()
    .hideAxis(["rating_id", "recommendation_id", "start_date", "end_date", "volume"])
    .brushMode("1D-axes-multi")
    .reorderable();

  previousTrades
    .dimension(ratingsByProvider)
    .group(function(d) { return "Previous Trades"; })
    .columns([
      function(d) { return d.currency_pair; },
      function(d) { return d.transaction_type; },
      function(d) { return d.rating; }
    ])
    .sortBy(function(d) { return d.rating; })
    .order(d3.descending);

  recommendationsTable
    .dimension(recommendationsByProvider)
    .group(function(d) { return "Recommendations"; })
    .columns([
      function(d) { return d.currency_pair; },
      function(d) { return d.transaction_type; },
      function(d) { return d.rating; },
      function(d) { return d.volume; }
    ])
    .size(10)
    .sortBy(function(d) { return d.rating; })
    .order(d3.descending);

  futureTrades
    .dimension(fratingsByProvider)
    .group(function(d) { return "Day after Trades"; })
    .columns([
      function(d) { return d.currency_pair; },
      function(d) { return d.transaction_type; },
      function(d) { return d.rating; }
    ])
    .sortBy(function(d) { return d.rating; })
    .order(d3.descending);

  // chordData = chordChartData(data);
  //
  // var ch = viz.ch().data(chordData).padding(.01)
  //   .innerRadius(330)
  //   .outerRadius(350)
  //   .chordOpacity(0.3)
  //   .labelOrientThreshold(1)
  //   .labelPadding(.03)
  //   .fill(d3.scale.category10());
  //
  // var svg = d3.select("svg#chord-data");
  // ch.defs(svg, 1);
  // svg.append("g").attr("transform", "translate(500,550)").call(ch);

  updateSelections(true);
  dc.renderAll();
}

function chordChartData(data) {
  var byProvider = {};
  data.forEach(function(d) {
    var currency = d.transaction_type + " " + d.currency_pair;
    if (d.provider_id in byProvider) {
      if (d.isRecommendation == "YES") {
        byProvider[d.provider_id].r.push([currency, 1]);
      }
      else {
        byProvider[d.provider_id].prev.push(currency);
      }
    }
    else {
      byProvider[d.provider_id] = {
        "prev": [],
        "r": []
      };
      if (d.isRecommendation == "YES") {
        byProvider[d.provider_id].r.push([currency, 1]);
      }
      else {
        byProvider[d.provider_id].prev.push(currency);
      }
    }
  });

  var byPrev = {};
  for (var provider in byProvider) {
    for (var i = 0; i < byProvider[provider].prev.length; i++) {
      var c = byProvider[provider].prev[i];
      if (c in byPrev) {
        byPrev[c] = merge(byPrev[c], byProvider[provider].r)
      }
      else {
        byPrev[c] = byProvider[provider].r;
      }
    }
  }

  var res = []
  for (var currency in byPrev) {
    for (var i = 0; i < byPrev[currency].length; i++) {
      var mean = d3.mean(byPrev[currency], function(d) { return d[1]; })
      if (byPrev[currency][i][1] > mean) {
        res.push([currency].concat(byPrev[currency][i]));
      }
    }
  }

  return res;
}

function merge(a, b) {
  var p = []
  var flag = true;
  for (var i = 0; i < b.length; i++) {
    for (var j = 0; j < a.length; j++) {
      if (a[j][0].localeCompare(b[i][0]) === 0) {
        ++a[j][1];
        flag = false;
        break;
      }
    }
    if (flag) {
      p.push(b[i]);
    }
    else {
      flag = true;
    }
  }
  return a.concat(p);
}

function formatData(data) {
  data.forEach(function(d) {
    d.provider_id = +d.provider_id;
    d.rating = +d.rating;
    if ("recommendation_id" in d) {
      d.isRecommendation = "YES";
      d.volume = +d.volume;
    }
    else {
      d.isRecommendation = "NO";
      d.volume = 0;
      delete d["weights.trade_ratings"];
      delete d["weights.provider_ratings"];
      delete d["weights.currency_ratings"];
      delete d["weights.country_ratings"];
      delete d["weights.pc_trade_count"];
      delete d["weights.pc_trade_duration"];
      delete d["weights.pc_net_pnl"];
      delete d["weights.p_trade_count"];
      delete d["weights.p_trade_duration"];
      delete d["weights.p_net_pnl"];
      delete d["weights.c_trade_count"];
      delete d["weights.c_trade_duration"];
      delete d["weights.c_net_pnl"];
      delete d["weights.l_trade_count"];
      delete d["weights.l_trade_duration"];
      delete d["weights.l_net_pnl"];
    }
  })
}

function rangeChanged() {
  var providerRange = document.getElementById("provider-range");
  var value = document.getElementById("provider-value");
  if ("0" !== providerRange.value) {
    provider = this.dataByProvider.group().orderNatural().all()[providerRange.value - 1].key;
    value.innerHTML = provider;
    highlightProvider(provider);
  }
  else {
    value.innerHTML = "All";
    highlightProvider(null);
  }

  dc.redrawAll();
}

function highlightProvider(provider) {
  if (provider) {
    this.parcoords.highlight(this.dataByProvider.filter(provider).top(Infinity));
    recommendationsByProvider.filter(provider);
    ratingsByProvider.filter(provider);
    fratingsByProvider.filter(provider);
  }
  else {
    this.parcoords.unhighlight();
    this.dataByProvider.filterAll();
    recommendationsByProvider.filter(0);
    ratingsByProvider.filter(0);
    fratingsByProvider.filter(0);
  }
}

function generateParcoordsDimensions() {
  return {
    "provider_id": { index: 0, title: "Provider ID" },
    "country": {index: 1, title: "Country" },
    "isRecommendation": { index: 2, title: "Recommendation" },
    "transaction_type": { index: 3, title: "Transation Type" },
    "currency_pair": { index: 4, title: "Currency Pair" },
    "rating": { index: 5, title: "Rating" }
  };
}

function changeSelection(selection) {
  var providerSelection = document.getElementById("provider-selection");
  if (providerSelection.value !== "All") {
    highlightProvider(providerSelection.value);
  }
  else {
    highlightProvider(null);
  }

  dc.redrawAll();
  drawRadarCharts();
}

function updateSelections(first) {
  var providerSelection = document.getElementById("provider-selection");

  if (first) {
    var pAll = document.createElement("option");
    pAll.text = "All";
    providerSelection.add(pAll);
  }
  this.dataByProvider.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    providerSelection.add(option);
  });
}
