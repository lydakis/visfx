d3.queue()
  .defer(d3.csv, "mock-data/features.csv")
  .await(makeGraphs);

var colors = d3.scale.category20b();
var color = function(d) { return colors(d.provider_id); };

var radarChartConfig = {
  w: 600,
  h: 600,
  maxValue: 2,
  levels: 10,
  ExtraWidthX: 300
}

function makeGraphs(error, data) {
  formatData(data);
  this.ft = crossfilter(data);
  this.data = data;
  this.featuresByProviderByPair = ft.dimension(function(d) {
    return d.country + " " + d.provider_id + " " + d.transaction_type + " " + d.currency_pair;
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

  setupInputRange()
  this.r = formatRadarChartData(data, averageFeaturesGroup, null);

  RadarChart.draw("#radar-overview", formatRadarChartData(data, averageFeaturesGroup, null), radarChartConfig);
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

function formatRadarChartData(data, averageFeatures, provider) {
  this.variance = {
    pc_trade_count: 0,
    pc_amount: 0,
    pc_net_pnl: 0,
    pc_trade_duration: 0,
    pc_pnl_per_amount: 0,
    p_trade_count: 0,
    p_net_pnl: 0,
    p_amount: 0,
    p_trade_duration: 0,
    p_pnl_per_amount: 0,
    c_trade_count: 0,
    c_amount: 0,
    c_net_pnl: 0,
    c_trade_duration: 0,
    c_pnl_per_amount: 0,
    l_trade_count: 0,
    l_amount: 0,
    l_net_pnl: 0,
    l_trade_duration: 0,
    l_pnl_per_amount: 0
  };
  var chartData = [];
  data.forEach(function(d) {
    variance.pc_trade_count += Math.pow(d.pc_trade_count - averageFeatures.value().pcTradeCountAvg, 2);
    variance.pc_amount += Math.pow(d.pc_amount - averageFeatures.value().pcAmountAvg, 2);
    variance.pc_net_pnl += Math.pow(d.pc_net_pnl - averageFeatures.value().pcNetPNLAvg, 2);
    variance.pc_trade_duration += Math.pow(d.pc_trade_duration - averageFeatures.value().pcTradeDurationAvg, 2);
    variance.pc_pnl_per_amount += Math.pow(d.pc_pnl_per_amount - averageFeatures.value().pcPNLPerAmountAvg, 2);
    variance.p_trade_count += Math.pow(d.p_trade_count - averageFeatures.value().pTradeCountAvg, 2);
    variance.p_net_pnl += Math.pow(d.p_net_pnl - averageFeatures.value().pNetPNLAvg, 2);
    variance.p_amount += Math.pow(d.p_amount - averageFeatures.value().pAmountAvg, 2);
    variance.p_trade_duration += Math.pow(d.p_trade_duration - averageFeatures.value().pTradeDurationAvg, 2);
    variance.p_pnl_per_amount += Math.pow(d.p_pnl_per_amount - averageFeatures.value().pPNLPerAmountAvg, 2);
    variance.c_trade_count += Math.pow(d.c_trade_count - averageFeatures.value().cTradeCountAvg, 2);
    variance.c_amount += Math.pow(d.c_amount - averageFeatures.value().cAmountAvg, 2);
    variance.c_net_pnl += Math.pow(d.c_net_pnl - averageFeatures.value().cNetPNLAvg, 2);
    variance.c_trade_duration += Math.pow(d.c_trade_duration - averageFeatures.value().cTradeDurationAvg, 2);
    variance.c_pnl_per_amount += Math.pow(d.c_pnl_per_amount - averageFeatures.value().cPNLPerAmountAvg, 2);
    variance.c_trade_count += Math.pow(d.c_trade_count - averageFeatures.value().cTradeCountAvg, 2);
    variance.l_amount += Math.pow(d.l_amount - averageFeatures.value().lAmountAvg, 2);
    variance.l_net_pnl += Math.pow(d.l_net_pnl - averageFeatures.value().lNetPNLAvg, 2);
    variance.l_trade_duration += Math.pow(d.l_trade_duration - averageFeatures.value().lTradeDurationAvg, 2);
    variance.l_pnl_per_amount += Math.pow(d.l_pnl_per_amount - averageFeatures.value().lPNLPerAmountAvg, 2);
  });
  variance.pc_trade_count /= averageFeatures.value().count;
  variance.pc_amount /= averageFeatures.value().count;
  variance.pc_net_pnl /= averageFeatures.value().count;
  variance.pc_trade_duration /= averageFeatures.value().count;
  variance.pc_pnl_per_amount /= averageFeatures.value().count;
  variance.p_trade_count /= averageFeatures.value().count;
  variance.p_net_pnl /= averageFeatures.value().count;
  variance.p_amount /= averageFeatures.value().count;
  variance.p_trade_duration /= averageFeatures.value().count;
  variance.p_pnl_per_amount /= averageFeatures.value().count;
  variance.c_trade_count /= averageFeatures.value().count;
  variance.c_amount /= averageFeatures.value().count;
  variance.c_net_pnl /= averageFeatures.value().count;
  variance.c_trade_duration /= averageFeatures.value().count;
  variance.c_pnl_per_amount /= averageFeatures.value().count;
  variance.l_trade_count /= averageFeatures.value().count;
  variance.l_amount /= averageFeatures.value().count;
  variance.l_net_pnl /= averageFeatures.value().count;
  variance.l_trade_duration /= averageFeatures.value().count;
  variance.l_pnl_per_amount /= averageFeatures.value().count;

  if (0) {
    chartData = [
      [
        {axis: "Times Pair traded by Provider", value: variance.pc_trade_count},
        // {axis: "Amount traded from Pair by Provider", value: variance.pc_amount},
        // {axis: "Total Net PnL from Pair by Provider", value: variance.pc_net_pnl},
        {axis: "Net PnL per Amount for Pair by Provider", value: variance.pc_pnl_per_amount},
        {axis: "Average Trade Duration for Pair by Provider", value: variance.pc_trade_duration},
        {axis: "Number of Trades made by Provider", value: variance.p_trade_count},
        // {axis: "Provider total Amount traded", value: variance.p_amount},
        // {axis: "Provider Net PnL", value: variance.p_net_pnl},
        {axis: "Provider Net PnL per Amount", value: variance.p_pnl_per_amount},
        {axis: "Provider Average Trade Duration", value: variance.p_trade_duration},
        {axis: "Times Pair traded in total", value: variance.c_trade_count},
        // {axis: "Amount Pair traded in total", value: variance.c_amount},
        // {axis: "Net PnL from Pair in total", value: variance.c_net_pnl},
        {axis: "Net PnL per Amount for Pair in total", value: variance.c_pnl_per_amount},
        {axis: "Pair Average Trade Duration", value: variance.c_trade_duration},
        {axis: "Number of trades by Country", value: variance.l_trade_count},
        // {axis: "Amount Pair traded in total", value: variance.l_amount},
        // {axis: "Net PnL from Pair in total", value: variance.l_net_pnl},
        {axis: "Net PnL per Amount of Country", value: variance.l_pnl_per_amount},
        {axis: "Average Trade Duration of Country", value: variance.l_trade_duration},
      ]
    ];
  }
  else {
    chartData = [
      [
        {axis: "Times Pair traded by Provider", value: averageFeatures.value().pcTradeCountAvg},
        // {axis: "Amount traded from Pair by Provider", value: averageFeatures.value().pcAmountAvg},
        // {axis: "Total Net PnL from Pair by Provider", value: averageFeatures.value().pcNetPNLAvg},
        {axis: "Net PnL per Amount for Pair by Provider", value: averageFeatures.value().pcPNLPerAmountAvg},
        {axis: "Average Trade Duration for Pair by Provider", value: averageFeatures.value().pcTradeDurationAvg},
        {axis: "Number of Trades made by Provider", value: averageFeatures.value().pTradeCountAvg},
        // {axis: "Provider Net PnL", value: averageFeatures.value().pNetPNLAvg},
        // {axis: "Provider total Amount traded", value: averageFeatures.value().pAmountAvg},
        {axis: "Provider Net PnL per Amount", value: averageFeatures.value().pPNLPerAmountAvg},
        {axis: "Provider Average Trade Duration", value: averageFeatures.value().pcTradeDurationAvg},
        {axis: "Times Pair traded in total", value: averageFeatures.value().cTradeCountAvg},
        // {axis: "Amount Pair traded in total", value: averageFeatures.value().cAmountAvg},
        // {axis: "Net PnL from Pair in total", value: averageFeatures.value().cNetPNLAvg},
        {axis: "Net PnL per Amount for Pair in total", value: averageFeatures.value().cPNLPerAmountAvg},
        {axis: "Pair Average Trade Duration", value: averageFeatures.value().cTradeDurationAvg},
        {axis: "Number of trades by Country", value: averageFeatures.value().lTradeCountAvg},
        // {axis: "Amount Pair traded in total", value: averageFeatures.value().lAmountAvg},
        // {axis: "Net PnL from Pair in total", value: averageFeatures.value().lNetPNLAvg},
        {axis: "Net PnL per Amount of Country", value: averageFeatures.value().lPNLPerAmountAvg},
        {axis: "Average Trade Duration of Country", value: averageFeatures.value().lTradeDurationAvg},
      ]
    ];
  }

  return chartData;
}

function rangeChanged() {
  var providerRange = document.getElementById("provider-range");
  var value = document.getElementById("provider-value");
  if ("0" !== providerRange.value) {
    providerPair = this.featuresByProviderByPair.group().orderNatural().all()[providerRange.value - 1].key;
    value.innerHTML = providerPair;
    this.featuresByProviderByPair.filter(providerPair);
    radarChartConfig.maxValue = 2;
    RadarChart.draw("#radar-overview", formatRadarChartData(this.data, this.averageFeaturesGroup, providerPair), radarChartConfig);
  }
  else {
    value.innerHTML = "All";
    this.featuresByProviderByPair.filterAll();
    radarChartConfig.maxValue = 0.0000000000001;
    RadarChart.draw("#radar-overview", formatRadarChartData(this.data, this.averageFeaturesGroup, null), radarChartConfig);
  }
}

function setupInputRange() {
  document.getElementById("provider-range")
    .setAttribute("max", this.featuresByProviderByPair.group().size());
}
