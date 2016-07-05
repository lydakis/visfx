d3.queue()
  .defer(d3.csv, 'mock-data/data.csv')
  .await(makeGraphs);

function makeGraphs(error, data) {
  var colors = d3.scale.category20b();

  var color = function(d) { return colors(d["Provider ID"]); };

  var parcoords = d3.parcoords()("#parcoords-example")
    .data(data)
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

  parcoords.svg.selectAll("text")
    .style("font", "10px sans-serif");
}
