// set the dimensions and margins of the graph
var outerWidth = 650;
var outerHeight = 300;

var margin = {top: 20, right: 20, bottom: 70, left: 100},
    width = outerWidth - margin.left - margin.right - 20,
    height = outerHeight - margin.top - margin.bottom;

// set the ranges
var x = d3.scaleLinear()
    .range([0, width]);
      
var y = d3.scaleBand()
    .range([height, 0])
    .padding(0.33);

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
x.domain([0, d3.max(data, d => d.value)])
y.domain(data.map(d => d.team));


// append the rectangles for the bar chart
var bar = svg.selectAll(".bar")
    .data(data)
    .join("g")
        .attr("class","bar")



var rect = bar.append('rect')
    .attr("width", d => x(d.value))
    .attr("y", d => y(d.team))
    .attr("height", y.bandwidth())

    .style('fill', d => d3.interpolatePurples(d.value/100))

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
    .attr('transform', `translate(${width/2},250)`)
    .text('Wins')

// y label
ylabel = labels.append('text')
    .attr('transform', `translate(-65,${height/2}) rotate(-90)`) 
    .text('Teams')

barLabels = bar.append('text')
    .attr('class', 'barlabel')
    .attr('x', d => x(d.value) - 20)
    .attr('y', d => y(d.team) + (y.bandwidth()/2) + 4)
    .text(d => d.value)
    .style('fill', 'black')




    


