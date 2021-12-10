// set a width and height for our SVG
var width = 640,
    height = 480;

// setup links
var links = [
        {source: '120.0.0.0', target: '120.0.0.2'},
    {source: '120.0.0.2', target: '120.0.0.3'},
    {source: '120.0.0.3', target: '120.0.0.4'},
    {source: '120.0.0.4', target: '120.0.0.5'},
    {source: '120.0.0.5', target: '120.0.0.6'},
    {source: '120.0.0.6', target: '120.0.0.0'},
];

      
    // create empty nodes array
    var nodes = {};
    
    // compute nodes from links data
    links.forEach(function(link) {
        link.source = nodes[link.source] ||
            (nodes[link.source] = {name: link.source});
        link.target = nodes[link.target] ||
            (nodes[link.target] = {name: link.target});        
    });


    // add a SVG to the body for our viz
    var svg=d3.select('body').append('svg')
        .attr('width', width)
        .attr('height', height);

    // use the force
    var force = d3.layout.force()
        .size([width, height])
        .nodes(d3.values(nodes))
        .links(links)
        .on("tick", tick)
        .linkDistance(100)
        .start();

    // add links
    var link = svg.selectAll('.link')
        .data(links)
        .enter().append('line')
        .attr('class', 'link'); 

    // add nodes
    var node = svg.selectAll('.node')
        .data(force.nodes())
        .enter().append('circle')
        .attr('class', 'node')
        .attr('r', width * 0.03);
   


    // what to do 
    function tick(e) {
        
        node.attr('cx', function(d) { return d.x; })
            .attr('cy', function(d) { return d.y; })
            .call(force.drag);
            
        link.attr('x1', function(d) { return d.source.x; })
            .attr('y1', function(d) { return d.source.y; })
            .attr('x2', function(d) { return d.target.x; })
            .attr('y2', function(d) { return d.target.y; });
        
    }