d3.queue()
  .defer(d3.csv, "mock-data/features.csv")
  .await(makeGraphs);

var colors = d3.scale.category10();
var color = function(d) { return colors(d.provider_id); };

var radarChartConfig = {
  w: 400,
  h: 400,
  maxValue: 2,
  levels: 10,
  ExtraWidthX: 200
}

function makeGraphs(error, data) {
  formatData(data);
  this.data = data;
  this.ft = crossfilter(data);

  this.featuresByTrade = ft.dimension(function(d) {
    return d.provider_id + " " + d.country + " " + d.transaction_type + " " +
      d.currency_pair;
  })

  this.featuresByProvider = ft.dimension(function(d) {
    return d.provider_id;
  });

  this.featuresByCurrency = ft.dimension(function(d) {
    return d.currency_pair;
  });

  this.featuresByType = ft.dimension(function(d) {
    return d.transaction_type;
  });

  this.featuresByCountry = ft.dimension(function(d) {
    return d.country;
  });

  this.averageFeaturesGroup = this.ft.dimension(function(d) {
    return d.country + " " + d.provider_id + " " + d.transaction_type + " " + d.currency_pair;
  })
    .groupAll()
    .reduce(
      function(p, v) {
        ++p.count;
        p.pcTradeCountSum += v.pc_trade_count;
        p.pcAmountSum += v.pc_amount;
        p.pcNetPNLSum += v.pc_net_pnl;
        p.pcTradeDurationSum += v.pc_trade_duration;
        p.pcPNLPerAmountSum += v.pc_pnl_per_amount;
        p.pTradeCountSum += v.p_trade_count;
        p.pNetPNLSum += v.p_net_pnl;
        p.pAmoutSum += v.p_amount;
        p.pTradeDurationSum += v.p_trade_duration;
        p.pPNLPerAmountSum += v.p_pnl_per_amount;
        p.cTradeCountSum += v.c_trade_count;
        p.cAmountSum += v.c_amount;
        p.cNetPNLSum += v.c_net_pnl;
        p.cTradeDurationSum += v.c_trade_duration;
        p.cPNLPerAmountSum += v.c_pnl_per_amount;
        p.lTradeCountSum += v.l_trade_count;
        p.lAmountSum += v.l_amount;
        p.lNetPNLSum += v.l_net_pnl;
        p.lTradeDurationSum += v.l_trade_duration;
        p.lPNLPerAmountSum += v.l_pnl_per_amount;

        p.pcTradeCountAvg = p.pcTradeCountSum / p.count;
        p.pcAmountAvg = p.pcAmountSum / p.count;
        p.pcNetPNLAvg = p.pcNetPNLSum / p.count;
        p.pcTradeDurationAvg = p.pcTradeCountSum / p.count;
        p.pcPNLPerAmountAvg = p.pcPNLPerAmountSum / p.count;
        p.pTradeCountAvg = p.pTradeCountSum / p.count;
        p.pNetPNLAvg = p.pNetPNLSum / p.count;
        p.pAmountAvg = p.pAmoutSum / p.count;
        p.pTradeDurationAvg = p.pTradeDurationSum / p.count;
        p.pPNLPerAmountAvg = p.pPNLPerAmountSum / p.count;
        p.cTradeCountAvg = p.cTradeCountSum / p.count;
        p.cAmountAvg = p.cAmountSum / p.count;
        p.cNetPNLAvg = p.cNetPNLSum / p.count;
        p.cTradeDurationAvg = p.cTradeCountSum / p.count;
        p.cPNLPerAmountAvg = p.cPNLPerAmountSum / p.count;
        p.lTradeCountAvg = p.lTradeCountSum / p.count;
        p.lAmountAvg = p.lAmountSum / p.count;
        p.lNetPNLAvg = p.lNetPNLSum / p.count;
        p.lTradeDurationAvg = p.lTradeCountSum / p.count;
        p.lPNLPerAmountAvg = p.lPNLPerAmountSum / p.count;
        return p;
      },
      function(p, v) {
        --p.count;
        p.pcTradeCountSum -= v.pc_trade_count;
        p.pcAmountSum -= v.pc_amount;
        p.pcNetPNLSum -= v.pc_net_pnl;
        p.pcTradeDurationSum -= v.pc_trade_duration;
        p.pcPNLPerAmountSum -= v.pc_pnl_per_amount;
        p.pTradeCountSum -= v.p_trade_count;
        p.pNetPNLSum -= v.p_net_pnl;
        p.pAmoutSum -= v.p_amount;
        p.pTradeDurationSum -= v.p_trade_duration;
        p.pPNLPerAmountSum -= v.p_pnl_per_amount;
        p.cTradeCountSum -= v.c_trade_count;
        p.cAmountSum -= v.c_amount;
        p.cNetPNLSum -= v.c_net_pnl;
        p.cTradeDurationSum -= v.c_trade_duration;
        p.cPNLPerAmountSum -= v.c_pnl_per_amount;
        p.lTradeCountSum -= v.l_trade_count;
        p.lAmountSum -= v.l_amount;
        p.lNetPNLSum -= v.l_net_pnl;
        p.lTradeDurationSum -= v.l_trade_duration;
        p.lPNLPerAmountSum -= v.l_pnl_per_amount;

        p.pcTradeCountAvg = p.pcTradeCountSum / p.count;
        p.pcAmountAvg = p.pcAmountSum / p.count;
        p.pcNetPNLAvg = p.pcNetPNLSum / p.count;
        p.pcTradeDurationAvg = p.pcTradeCountSum / p.count;
        p.pcPNLPerAmountAvg = p.pcPNLPerAmountSum / p.count;
        p.pTradeCountAvg = p.pTradeCountSum / p.count;
        p.pNetPNLAvg = p.pNetPNLSum / p.count;
        p.pAmountAvg = p.pAmoutSum / p.count;
        p.pTradeDurationAvg = p.pTradeDurationSum / p.count;
        p.pPNLPerAmountAvg = p.pPNLPerAmountSum / p.count;
        p.cTradeCountAvg = p.cTradeCountSum / p.count;
        p.cAmountAvg = p.cAmountSum / p.count;
        p.cNetPNLAvg = p.cNetPNLSum / p.count;
        p.cTradeDurationAvg = p.cTradeCountSum / p.count;
        p.cPNLPerAmountAvg = p.cPNLPerAmountSum / p.count;
        p.lTradeCountAvg = p.lTradeCountSum / p.count;
        p.lAmountAvg = p.lAmountSum / p.count;
        p.lNetPNLAvg = p.lNetPNLSum / p.count;
        p.lTradeDurationAvg = p.lTradeCountSum / p.count;
        p.lPNLPerAmountAvg = p.lPNLPerAmountSum / p.count;
        return p;
      },
      function() {
        return {
          count: 0,
          pcTradeCountSum: 0,
          pcAmountSum: 0,
          pcNetPNLSum: 0,
          pcTradeDurationSum:0,
          pcPNLPerAmountSum: 0,
          pTradeCountSum: 0,
          pNetPNLSum: 0,
          pAmoutSum: 0,
          pTradeDurationSum:0,
          pPNLPerAmountSum: 0,
          cTradeCountSum: 0,
          cAmountSum: 0,
          cNetPNLSum: 0,
          cTradeDurationSum:0,
          cPNLPerAmountSum: 0,
          lTradeCountSum: 0,
          lAmountSum: 0,
          lNetPNLSum: 0,
          lTradeDurationSum:0,
          lPNLPerAmountSum: 0,

          pcTradeCountAvg: 0,
          pcAmountAvg: 0,
          pcNetPNLAvg: 0,
          pcTradeDurationAvg: 0,
          pcPNLPerAmountAvg: 0,
          pTradeCountAvg: 0,
          pNetPNLAvg: 0,
          pAmountAvg: 0,
          pTradeDurationAvg: 0,
          pPNLPerAmountAvg: 0,
          cTradeCountAvg: 0,
          cAmountAvg: 0,
          cNetPNLAvg: 0,
          cTradeDurationAvg: 0,
          cPNLPerAmountAvg: 0,
          lTradeCountAvg: 0,
          lAmountAvg: 0,
          lNetPNLAvg: 0,
          lTradeDurationAvg: 0,
          lPNLPerAmountAvg: 0
        }
      }
    );

  setupInputRange();
  updateSelections(true);
  drawRadarCharts();
}

function formatData(data) {
  var dateFormat = d3.time.format("%Y-%m-%d");

  data.forEach(function(d) {
    d.provider_id = +d.provider_id;
    d.pc_trade_count = +d.pc_trade_count + 1;
    d.pc_amount = +d.pc_amount + 1;
    d.pc_net_pnl = +d.pc_net_pnl + 1;
    d.pc_trade_duration = +d.pc_trade_duration + 1;
    d.pc_pnl_per_amount = +d.pc_pnl_per_amount + 1;
    d.p_trade_count = +d.p_trade_count + 1;
    d.p_amount = +d.p_amount + 1;
    d.p_net_pnl = +d.p_net_pnl + 1;
    d.p_trade_duration = +d.p_trade_duration + 1;
    d.p_pnl_per_amount = +d.p_pnl_per_amount + 1;
    d.c_trade_count = +d.c_trade_count + 1;
    d.c_amount = +d.c_amount + 1;
    d.c_net_pnl = +d.c_net_pnl + 1;
    d.c_trade_duration = +d.c_trade_duration + 1;
    d.c_pnl_per_amount = +d.c_pnl_per_amount + 1;
    d.l_trade_count = +d.l_trade_count + 1;
    d.l_amount = +d.l_amount + 1;
    d.l_net_pnl = +d.l_net_pnl + 1;
    d.l_trade_duration = +d.l_trade_duration + 1;
    d.l_pnl_per_amount = +d.l_pnl_per_amount + 1;
    d.start_date = dateFormat.parse(d.start_date);
    d.end_date = dateFormat.parse(d.end_date);
  });
}

function formatRadarChartData(averageFeatures, type) {
  var chartData = [];

  if ("pc" === type) {
    chartData = [[
      {axis: "Trade Count", value: averageFeatures.value().pcTradeCountAvg},
      {axis: "Amount", value: averageFeatures.value().pcAmountAvg},
      {axis: "Net PnL", value: averageFeatures.value().pcNetPNLAvg},
      {axis: "Net PnL per Amount", value: averageFeatures.value().pcPNLPerAmountAvg},
      {axis: "Trade Duration", value: averageFeatures.value().pcTradeDurationAvg}
    ]];
  }
  else if ("p" === type) {
    chartData = [[
      {axis: "Trade Count", value: averageFeatures.value().pTradeCountAvg},
      {axis: "Amount", value: averageFeatures.value().pNetPNLAvg},
      {axis: "Net PnL", value: averageFeatures.value().pAmountAvg},
      {axis: "Net PnL per Amount", value: averageFeatures.value().pPNLPerAmountAvg},
      {axis: "Trade Duration", value: averageFeatures.value().pcTradeDurationAvg}
      ]];
  }
  else if ("c" === type) {
    chartData = [[
      {axis: "Trade Count", value: averageFeatures.value().cTradeCountAvg},
      {axis: "Amount", value: averageFeatures.value().cAmountAvg},
      {axis: "Net PnL", value: averageFeatures.value().cNetPNLAvg},
      {axis: "Net PnL per Amount", value: averageFeatures.value().cPNLPerAmountAvg},
      {axis: "Trade Duration", value: averageFeatures.value().cTradeDurationAvg}
    ]];
  }
  else if ("l" === type) {
    chartData = [[
      {axis: "Trade Count", value: averageFeatures.value().lTradeCountAvg},
      {axis: "Amount", value: averageFeatures.value().lAmountAvg},
      {axis: "Net PnL", value: averageFeatures.value().lNetPNLAvg},
      {axis: "Net PnL per Amount", value: averageFeatures.value().lPNLPerAmountAvg},
      {axis: "Trade Duration", value: averageFeatures.value().lTradeDurationAvg}
    ]]
  }

  return chartData;
}

function rangeChanged() {
  var providerRange = document.getElementById("trade-range");
  var value = document.getElementById("trade-value");
  if ("0" !== providerRange.value) {
    providerPair = this.featuresByTrade.group().orderNatural().all()[providerRange.value - 1].key;
    value.innerHTML = providerPair;
    this.featuresByTrade.filter(providerPair);
    drawRadarCharts();
  }
  else {
    value.innerHTML = "All";
    this.featuresByTrade.filterAll();
    drawRadarCharts();
  }
}

function changeSelection(selection) {
  var providerSelection = document.getElementById("provider-selection");
  var countrySelection = document.getElementById("country-selection");
  var typeSelection = document.getElementById("type-selection");
  var currencySelection = document.getElementById("currency-selection");
  console.log("here!");
  this.featuresByProvider.filter(
    providerSelection.value !== "All" ? +providerSelection.value : null);
  this.featuresByCountry.filter(
    countrySelection.value !== "All" ? countrySelection.value : null);
  this.featuresByType.filter(
    typeSelection.value !== "All" ? typeSelection.value : null);
  this.featuresByCurrency.filter(
    currencySelection.value !== "All" ? currencySelection.value : null);

  // emptySelections(selection);
  // updateSelections(false);
  drawRadarCharts();
}

function drawRadarCharts() {
  radarChartConfig.color = colors(1)
  RadarChart.draw("#radar-pc", formatRadarChartData(this.averageFeaturesGroup, 'pc'), radarChartConfig);
  radarChartConfig.color = colors(2)
  RadarChart.draw("#radar-p", formatRadarChartData(this.averageFeaturesGroup, 'p'), radarChartConfig);
  radarChartConfig.color = colors(3)
  RadarChart.draw("#radar-c", formatRadarChartData(this.averageFeaturesGroup, 'c'), radarChartConfig);
  radarChartConfig.color = colors(4)
  RadarChart.draw("#radar-l", formatRadarChartData(this.averageFeaturesGroup, 'l'), radarChartConfig);
}

function setupInputRange() {
  document.getElementById("trade-range")
    .setAttribute("max", this.featuresByTrade.group().size());
}

function updateSelections(first) {
  var providerSelection = document.getElementById("provider-selection");
  var countrySelection = document.getElementById("country-selection");
  var typeSelection = document.getElementById("type-selection");
  var currencySelection = document.getElementById("currency-selection");

  if (first) {
    var pAll = document.createElement("option");
    pAll.text = "All";
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
  this.featuresByProvider.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    providerSelection.add(option);
  });
  this.featuresByCountry.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    countrySelection.add(option);
  });
  this.featuresByType.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    typeSelection.add(option);
  });
  this.featuresByCurrency.group().orderNatural().all().forEach(function(d) {
    var option = document.createElement("option");
    option.text = d.key;
    currencySelection.add(option);
  });
}

function emptySelections(selection) {
  var providerSelection = document.getElementById("provider-selection");
  var countrySelection = document.getElementById("country-selection");
  var typeSelection = document.getElementById("type-selection");
  var currencySelection = document.getElementById("currency-selection");

  if (selection !== providerSelection) {
    var length = providerSelection.options.length;
    for (var i = 1; i < length; i++) {
      providerSelection.remove(i);
    }
  }
  if (selection !== countrySelection) {
    var length = countrySelection.options.length;
    for (var i = 1; i < length; i++) {
      countrySelection.remove(i);
    }
  }
  if (selection !== typeSelection) {
    var length = typeSelection.options.length;
    for (var i = 1; i < length; i++) {
      typeSelection.remove(i);
    }
  }
  if (selection !== currencySelection) {
    var length = currencySelection.options.length;
    for (var i = 1; i < length; i++) {
      currencySelection.remove(i);
    }
  }
}
