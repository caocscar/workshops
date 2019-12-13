// set the dimensions and margins of the graph
var outerWidth = 960;
var outerHeight = 500;

var margin = {top: 50, right: 20, bottom: 80, left: 80},
    width = outerWidth - margin.left - margin.right,
    height = outerHeight - margin.top - margin.bottom;

// set the ranges
var x = d3.scaleBand()
    .range([0, width])
    .padding(0.33);

var y= d3.scaleLinear()
    .range([height, 0]);
    

var xAxis = d3.axisTop(x)
    .ticks(5)

var yAxis = d3.axisLeft(y)
    .tickFormat('')

// append the svg object to the body of the page
// append a 'group' element to 'svg'
// moves the 'group' element to the top left margin
var svg = d3.select('body').append('svg')
    .attr("class", "chart")
    .attr("width", outerWidth)
    .attr("height", outerHeight)
  .append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);
 
// data 
var data = [{'team':'Boston','value':100},
        {'team':'Detroit','value':85},
        {'team':'New York','value':80},
        {'team':'Atlanta','value':75}, 
        {'team':'Chicago','value':30}]


// scale the range of the data in the domains 
x.domain(data.map(d => d.team));
y.domain([0, d3.max(data, d => d.value)])


// append the rectangles for the bar chart
var bar = svg.selectAll(".bar")
    .data(data)
    .join("g")
        .attr("class","bar")



var rect = bar.append('rect')
    .attr("height", d => height - y(d.value))
    .attr("x", d => x(d.team))
    .attr("width", x.bandwidth())
    .attr("y", d => y(d.value))
    .style('fill', d => d3.interpolatePurples(d.value/100));

// add the x Axis
svg.append("g")
    .attr("transform", `translate(0, ${height})`)
    .call(d3.axisBottom(x));

// add the y Axis
svg.append("g")
    .call(d3.axisLeft(y));

// add chart labels 
labels = svg.append('g')
    .attr('class', 'label')

// x label
labels.append('text')
    .attr('transform', `translate(${width/2},450)`)
    .text('Teams')

// y label
ylabel = labels.append('text')
    .attr('transform', `translate(-45,${height/2}) rotate(-90)`) 
    .text('Wins')

barLabels = bar.append('text')
    .attr('class', 'barlabel')
    .attr('x', d => x(d.team) + (x.bandwidth()/2)) 
    .attr('y', d => y(d.value) - 15)
    .text(d => d.value)
    .style('fill', 'black')
