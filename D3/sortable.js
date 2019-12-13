async function createChart() {
    
  // read data
  const fileLocation = 'https://gist.githubusercontent.com/caocscar/8cdb75721ea4f6c8a032a00ebc73516c/raw/854bbee2faffb4f6947b6b6c2424b18ca5a8970e/mlb2018.csv'
  DATA = await d3.csv(fileLocation, type)
  let chartDate = new Date(2018,3,3)
  let data = filterData(chartDate)
  
  // margins
  let margin = {top: 80, right: 90, bottom: 30+50, left: 120},
    width = 900 - margin.left - margin.right,
    height = 1500 - margin.top - margin.bottom; // 760

  // svg setup
  let svg = d3.select('body').append('svg')
      .attr("class", "chart")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
    .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

  // set up scales
  let y = d3.scaleBand()
      .domain(data.map(d => d.team).reverse())
      .range([height, 0])
      .padding(0.33)

  let x = d3.scaleLinear()
      .domain([0, Math.ceil(d3.max(data, d => d.value)/5)*5])
      .range([0, width]);
      
  // add axes
  let xAxis = d3.axisTop(x)
      .ticks(6)

  svg.append("g")
      .attr("class", "x axis")
      .call(xAxis);

  let yAxis = d3.axisLeft(y)
      .tickFormat('')

  svg.append("g")
      .attr("class", "y axis")
      .call(yAxis);

  // add the x-axis gridlines
  let gridlines = d3.axisTop(x)
      .ticks(6)
      .tickSize(-height)
      .tickFormat("")

  svg.append("g")			
      .attr("class", "grid")
      .call(gridlines)

  // set up bar groups
  let bar = svg.selectAll(".bar")
    .data(data)
    .join("g")
      .attr("class", "bar")
      .attr("transform", d => `translate(0,${y(d.team)})`)

  // adding bars
  let rects = bar.append('rect')
      .attr("width", (d,i) => x(d.value))
      .attr("height", y.bandwidth())
      .style('fill', d => d3.interpolateRdYlBu(d.value/100))

  // team labels
  bar.append('text')
      .attr('class', 'team')
      .attr('x', -10)
      .attr('y', y.bandwidth()/2 + 5)
      .text(d => d.team)

  // team logos
  const imgsize = 40
  let imgs = bar.append("svg:image")
      .attr('class', 'logo')
      .attr('x', d => x(d.value) + 5)
      .attr('y', -5)
      .attr('width', imgsize)
      .attr('height', imgsize)
      .attr("xlink:href", d => `http://www.capsinfo.com/images/MLB_Team_Logos/${urls[d.team]}.png`)
  
  // bar labels
  let barLabels = bar.append('text')
      .attr('class', 'barlabel')
      .attr('x', d => x(d.value) + 10 + imgsize)
      .attr('y', y.bandwidth()/2 + 5)
      .text(d => d.value)

  // other chart labels
  labels = svg.append('g')
      .attr('class', 'label')

  // x label
  labels.append('text')
      .attr('transform', `translate(${width},-40)`)
      .text('Wins')

  // y label
  ylabel = labels.append('text')
      .attr('transform', `translate(-80,${height/2}) rotate(-90)`) // order matters
      .text('Teams')

  // date label
  const formatDate = d3.timeFormat('%b %-d')
  let dateLabel = labels.append('text')
      .attr('id', 'date')
      .attr('transform', 'translate(0,-40)')
      .text(formatDate(chartDate))

  labels.append('text')
      .attr('id', 'season')
      .attr('transform', `translate(${width/2},-40)`)
      .text('MLB 2018 Season')

  // clipping rectangle
  const z = 0.97*(height / data.length)
  d3.select('.chart').append("defs")
    .append("clipPath")
      .attr("id", "clip")
    .append("rect")
      .attr('x', 0)
      .attr('y', 0)
      .attr("width", width + margin.left + margin.right)
      .attr("height", 0.4*height)    

  // sorting transition
  const T = 300
  let dailyUpdate = setInterval(function() {

    chartDate = d3.timeDay.offset(chartDate,1)
    dateLabel.text(formatDate(chartDate))
    data = filterData(chartDate)

    // update x-axis
    x.domain([0, Math.ceil(d3.max(data, d => d.value)/5)*5]);
    svg.select('.x.axis').transition().duration(T)
        .call(xAxis);
    svg.select('.grid').transition().duration(T)
        .call(gridlines);

    // update bar chart
    rects.data(data)
      .transition().duration(T)
        .attr("width", d => x(d.value))
        .style('fill', d => d3.interpolateRdYlBu(d.value/100))
    imgs.data(data)
      .transition().duration(T)
        .attr('x', d => x(d.value) + 5)
    barLabels.data(data)
      .transition().duration(T)
        .attr('x', d => x(d.value) + 10 + imgsize)
        .text(d => d.value)
    
    // sort data
    data.sort((a,b) => d3.descending(a.value,b.value));

    // update y-axis
    y.domain(data.map(d => d.team).reverse());
    bar.transition().duration(T)
        .attr("transform", d => `translate(0,${y(d.team)})`)

    // exit function
    if (chartDate > new Date(2018,9,1)) {
      clearInterval(dailyUpdate)
    }

  }, T);

}

function type(d) {
  const formatDate = d3.timeParse('%Y%m%d')
  d.date = formatDate(d.date)
  return d
}

function filterData(chartDate) {
  const snapshot = DATA.filter(d => d.date <= chartDate)
  const wins = d3.rollup(snapshot, v => v.length, d => d.team) // returns Map object
  return Array.from(wins, ([key, value]) => ({'team':key, 'value':value}))
}