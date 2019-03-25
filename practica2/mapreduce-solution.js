/********************************************************
 * Map Reduce                                           *
 ********************************************************/

// Map function
var m = function() { 
  // Distance between two coordinates in Km
  // http://www.movable-type.co.uk/scripts/latlong.html
  var d = function(lat1, lon1, lat2, lon2) {
    var R = 6371; // Radius of the earth in km
    var dLat = (lat2-lat1) * (Math.PI/180); 
    var dLon = (lon2-lon1) * (Math.PI/180); 
    var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(lat1 * (Math.PI/180)) * Math.cos(lat2 * (Math.PI/180)) * 
            Math.sin(dLon/2) * Math.sin(dLon/2); 
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
    return R * c; // Distance in km
  }

  var cities = [ [52.371807, 4.896029, "Amsterdam"],
                 [41.390205, 2.154007, "Barcelona"],
                 [52.520008, 13.404954, "Berlin"],
                 [53.350140, -6.266155, "Dublin"],
                 [51.509865, -0.118092, "London"],
                 [40.416775, -3.703790, "Madrid"],
                 [48.864716, 2.349014, "Paris"],
                 [41.906204, 12.507516, "Rome"],
                 [47.451542, 8.564572, "Zurich"] ];

  var lat1 = this.Lat;
  var lon1 = this.Long;
  var radius = 50 // Km

  for (var i in cities) {
    var lat2 = cities[i][0];
    var lon2 = cities[i][1];
    var city = cities[i][2];
    if ( d(lat1, lon1, lat2, lon2) <= radius) emit (city, {"airplanes" : [this.Icao]});
  }
};  

// Reduce function
var r = function(key, values) { 
  var airplanes = [];
  for (var i in values) {
    airplanes = airplanes.concat(values[i].airplanes.filter(
           function (item) {return airplanes.indexOf(item) < 0;}));
  }
  return {"airplanes" : airplanes}; 
};

// Finalize function
var f = function(key,value) {
  return value.airplanes.length;
}


db.runCommand( {
                 mapReduce: "adsb",
                 map: m,
                 reduce: r,
                 finalize: f,
                 out: {replace : "result"}
               } );
