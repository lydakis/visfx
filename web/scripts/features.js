d3.queue()
  .defer(d3.csv, "mock-data/features.csv")
  .await(makeGraphs);

var colors = d3.scale.category20b();
var color = function(d) { return colors(d.provider_id); };

var radarChartConfig = {
  w: 600,
  h: 600,
  maxValue: 1,
  levels: 10,
  ExtraWidthX: 300
}

function makeGraphs(error, data) {
  formatData(data);
  this.ft = crossfilter(data);
  this.data = data;
  this.featuresByProviderByPair = ft.dimension(function(d) {
    return d.provider_id + " " + d.currency_pair;
  });

  this.averageFeaturesGroup = this.ft.dimension(function(d) {
    return d.provider_id + " " + d.currency_pair;
  })
    .groupAll()
    .reduce(
      function(p, v) {
        ++p.count;
        p.pairCountsSum += v.pair_counts + 1;
        p.totalAmountSum += v.total_amount + 1;
        p.totalPairPnlSum += v.total_pair_pnl + 1;
        p.pnlPerAmountSum += v.pnl_per_amount + 1;
        p.providerPnlSum += v.provider_pnl + 1;
        p.providerAmountSum += v.provider_amount + 1;
        p.providerPnlPerAmountSum += v.provider_pnl_per_amount + 1;
        // p.totalPairCountSum += v.total_pair_count + 1;
        p.pairAmountSum += v.pair_amount + 1;
        p.pairPnlSum += v.pair_pnl + 1;
        p.pairPnlPerAmountSum += v.pair_pnl_per_amount + 1;

        p.pairCountsAvg = p.pairCountsSum / p.count;
        p.totalAmountAvg = p.totalAmountSum / p.count;
        p.totalPairPnlAvg = p.totalPairPnlSum / p.count;
        p.pnlPerAmountAvg = p.pnlPerAmountSum / p.count;
        p.providerPnlAvg = p.providerPnlSum / p.count;
        p.providerAmountAvg = p.providerAmountSum / p.count;
        p.providerPnlPerAmountAvg = p.providerPnlPerAmountSum / p.count;
        // p.totalPairCountAvg = p.totalPairCountSum / p.count;
        p.pairAmountAvg = p.pairAmountSum / p.count;
        p.pairPnlAvg = p.pairPnlSum / p.count;
        p.pairPnlPerAmountAvg = p.pairPnlPerAmountSum / p.count;
        return p;
      },
      function(p, v) {
        --p.count;
        p.pairCountsSum -= v.pair_counts + 1;
        p.totalAmountSum -= v.total_amount + 1;
        p.totalPairPnlSum -= v.total_pair_pnl + 1;
        p.pnlPerAmountSum -= v.pnl_per_amount + 1;
        p.providerPnlSum -= v.provider_pnl + 1;
        p.providerAmountSum -= v.provider_amount + 1;
        p.providerPnlPerAmountSum -= v.provider_pnl_per_amount + 1;
        // p.totalPairCountSum -= v.total_pair_count + 1;
        p.pairAmountSum -= v.pair_amount + 1;
        p.pairPnlSum -= v.pair_pnl + 1;
        p.pairPnlPerAmountSum -= v.pair_pnl_per_amount + 1;

        p.pairCountsAvg = p.pairCountsSum / p.count;
        p.totalAmountAvg = p.totalAmountSum / p.count;
        p.totalPairPnlAvg = p.totalPairPnlSum / p.count;
        p.pnlPerAmountAvg = p.pnlPerAmountSum / p.count;
        p.providerPnlAvg = p.providerPnlSum / p.count;
        p.providerAmountAvg = p.providerAmountSum / p.count;
        p.providerPnlPerAmountAvg = p.providerPnlPerAmountSum / p.count;
        // p.totalPairCountAvg = p.totalPairCountSum / p.count;
        p.pairAmountAvg = p.pairAmountSum / p.count;
        p.pairPnlAvg = p.pairPnlSum / p.count;
        p.pairPnlPerAmountAvg = p.pairPnlPerAmountSum / p.count;
        return p;
      },
      function() {
        return {
          count: 0,
          pairCountsSum: 0,
          totalAmountSum: 0,
          totalPairPnlSum: 0,
          pnlPerAmountSum: 0,
          providerPnlSum: 0,
          providerAmountSum: 0,
          providerPnlPerAmountSum: 0,
          // totalPairCountSum: 0,
          pairAmountSum: 0,
          pairPnlSum: 0,
          pairPnlPerAmountSum: 0,

          pairCountsAvg: 0,
          totalAmountAvg: 0,
          totalPairPnlAvg: 0,
          pnlPerAmountAvg: 0,
          providerPnlAvg: 0,
          providerAmountAvg: 0,
          providerPnlPerAmountAvg: 0,
          // totalPairCountAvg: 0,
          pairAmountAvg: 0,
          pairPnlAvg: 0,
          pairPnlPerAmountAvg: 0
        }
      }
    );

  setupInputRange()
  this.r = formatRadarChartData(data, averageFeaturesGroup, null);

  RadarChart.draw("#radar-overview", formatRadarChartData(data, averageFeaturesGroup, null), radarChartConfig);
}

function formatData(data) {
  var dateFormat = d3.time.format("%Y-%m-%dT%H:%M:%S");

  data.forEach(function(d) {
    d.provider_id = +d.provider_id;
    d.pair_counts = +d.pair_counts;
    d.total_amount = +d.total_amount;
    d.total_pair_pnl = +d.total_pair_pnl;
    d.pnl_per_amount = +d.pnl_per_amount;
    d.provider_pnl = +d.provider_pnl;
    d.provider_amount = +d.provider_amount;
    d.provider_pnl_per_amount = +d.provider_pnl_per_amount;
    // d.total_pair_count = +d.total_pair_count;
    d.pair_amount = +d.pair_amount;
    d.pair_pnl = +d.pair_pnl;
    d.pair_pnl_per_amount = +d.pair_pnl_per_amount;
    d.start_date = dateFormat.parse(d.start_date);
    d.end_date = dateFormat.parse(d.end_date);
    d.rating = +d.rating;
  });
}

function formatRadarChartData(data, averageFeatures, provider) {
  this.variance = {
    pair_counts: 0,
    total_amount: 0,
    total_pair_pnl: 0,
    pnl_per_amount: 0,
    provider_pnl: 0,
    provider_amount: 0,
    provider_pnl_per_amount: 0,
    // total_pair_count: 0,
    pair_amount: 0,
    pair_pnl_per_amount: 0,
    pair_pnl_per_amount: 0
  };
  var chartData = [];
  data.forEach(function(d) {
    variance.pair_counts += Math.pow(d.pair_counts + 1 - averageFeatures.value().pairCountsAvg, 2);
    variance.total_amount += Math.pow(d.total_amount + 1 - averageFeatures.value().totalAmountAvg, 2);
    variance.total_pair_pnl += Math.pow(d.total_pair_pnl + 1 - averageFeatures.value().totalPairPnlAvg, 2);
    variance.pnl_per_amount += Math.pow(d.pnl_per_amount + 1 - averageFeatures.value().pairPnlPerAmountAvg, 2);
    variance.provider_pnl += Math.pow(d.provider_pnl + 1 - averageFeatures.value().providerPnlAvg, 2);
    variance.provider_amount += Math.pow(d.provider_amount + 1 - averageFeatures.value().providerAmountAvg, 2);
    variance.provider_pnl_per_amount += Math.pow(d.provider_pnl_per_amount + 1 - averageFeatures.value().providerPnlPerAmountAvg, 2);
    // variance.total_pair_count += Math.pow(d.total_pair_count + 1 - averageFeatures.value().totalPairCountAvg, 2);
    variance.pair_amount += Math.pow(d.pair_amount + 1 - averageFeatures.value().pairAmountAvg, 2);
    variance.pair_pnl_per_amount += Math.pow(d.pair_pnl_per_amount + 1 - averageFeatures.value().pairPnlAvg, 2);
    variance.pair_pnl_per_amount += Math.pow(d.pair_pnl_per_amount + 1 - averageFeatures.value().pairPnlPerAmountAvg, 2);
  });
  variance.pair_counts /= averageFeatures.value().count * 0.0001;
  variance.total_amount /= averageFeatures.value().count * 0.0001;
  variance.total_pair_pnl /= averageFeatures.value().count * 0.0001;
  variance.pnl_per_amount /= averageFeatures.value().count * 0.0001;
  variance.provider_pnl /= averageFeatures.value().count * 0.0001;
  variance.provider_amount /= averageFeatures.value().count * 0.0001;
  variance.provider_pnl_per_amount /= averageFeatures.value().count * 0.0001;
  // variance.total_pair_count /= averageFeatures.value().count * 0.0001;
  variance.pair_amount /= averageFeatures.value().count * 0.0001;
  variance.pair_pnl_per_amount /= averageFeatures.value().count * 0.0001;
  variance.pair_pnl_per_amount /= averageFeatures.value().count * 0.0001;

  if (!provider) {
    chartData = [
      [
        {axis: "Times Pair traded by Provider", value: variance.pair_counts},
        // {axis: "Amount traded from Pair by Provider", value: variance.total_amount},
        // {axis: "Total Net PnL from Pair by Provider", value: variance.total_pair_pnl},
        {axis: "Net PnL per Amount for Pair by Provider", value: variance.pnl_per_amount},
        // {axis: "Provider Net PnL", value: variance.provider_pnl},
        // {axis: "Provider total Amount traded", value: variance.provider_amount},
        {axis: "Provider Net PnL per Amount", value: variance.provider_pnl_per_amount},
        // {axis: "Times Pair traded in total", value: variance.total_pair_count},
        // {axis: "Amount Pair traded in total", value: variance.pair_amount},
        // {axis: "Net PnL from Pair in total", value: variance.pair_pnl_per_amount},
        {axis: "Net PnL per Amount for Pair in total", value: variance.pair_pnl_per_amount}
      ]
    ];
  }
  else {
    chartData = [
      [
        {axis: "Times Pair traded by Provider", value: averageFeatures.value().pairCountsAvg},
        // {axis: "Amount traded from Pair by Provider", value: averageFeatures.value().totalAmountAvg},
        // {axis: "Total Net PnL from Pair by Provider", value: averageFeatures.value().totalPairPnlAvg},
        {axis: "Net PnL per Amount for Pair by Provider", value: averageFeatures.value().pairPnlPerAmountAvg},
        // {axis: "Provider Net PnL", value: averageFeatures.value().providerPnlAvg},
        // {axis: "Provider total Amount traded", value: averageFeatures.value().providerAmountAvg},
        {axis: "Provider Net PnL per Amount", value: averageFeatures.value().providerPnlPerAmountAvg},
        // {axis: "Times Pair traded in total", value: averageFeatures.value().totalPairCountAvg},
        // {axis: "Amount Pair traded in total", value: averageFeatures.value().pairAmountAvg},
        // {axis: "Net PnL from Pair in total", value: averageFeatures.value().pairPnlAvg},
        {axis: "Net PnL per Amount for Pair in total", value: averageFeatures.value().pairPnlPerAmountAvg}
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
    RadarChart.draw("#radar-overview", formatRadarChartData(this.data, this.averageFeaturesGroup, providerPair), radarChartConfig);
  }
  else {
    value.innerHTML = "All";
    this.featuresByProviderByPair.filterAll();
    RadarChart.draw("#radar-overview", formatRadarChartData(this.data, this.averageFeaturesGroup, null), radarChartConfig);
  }
}

function setupInputRange() {
  document.getElementById("provider-range")
    .setAttribute("max", this.featuresByProviderByPair.group().size());
}
