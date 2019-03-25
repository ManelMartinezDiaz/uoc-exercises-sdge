// *** 3T Software Labs, Studio 3T: MapReduce Job ****

// Variable for db
var __3tsoftwarelabs_db = "uoc";

// Variable for map
var __3tsoftwarelabs_map = function () {

    // Enter the JavaScript for the map function here
    // You can access the current document as 'this'
    //
    // Available functions: assert(), BinData(), DBPointer(), DBRef(), doassert(), emit(), gc()
    //                      HexData(), hex_md5(), isNumber(), isObject(), ISODate(), isString()
    //                      Map(), MD5(), NumberInt(), NumberLong(), ObjectId(), print()
    //                      printjson(), printjsononeline(), sleep(), Timestamp(), tojson()
    //                      tojsononeline(), tojsonObject(), UUID(), version()
    //
    // Available properties: args, MaxKey, MinKey

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

//Punts a afegir:
//1.En el map s’ha de fer un emit per cada registre ADS-B que estigui dins del radi d’una ciutat. 
//S’haurà d’indicar el codi ICAO de l’avió que l’ha generat per després poder comptar els avions. 
//Aquest codi ICAO és únic per aeronau.
//var radiZona = 50, Radi de zona en Km
//Output: Key (Ciutat) i Value (icao) que Señal < 50 km de Ciutat.

	var radiZona = 50; 
	var result = {Icao: [], count: ""};

  	for (i in cities) {
    	var distanciaCity = d(cities[i][0],cities[i][1],this.Lat,this.Long);

    	if (distanciaCity < radiZona) {
      	var key = cities[i][2];
      	var value = {Icao: this.Icao, count: 1};
     	//print("Report: ", distanciaCity,"city; ", cities[i][2],"ICAO:", value.Icao,"Count", value.count);
      	emit(key,value);
    	}
  	}

};

// Variable for reduce
var __3tsoftwarelabs_reduce = function (key, values) {

    // Enter the JavaScript for the reduce function here
    // 'values' is a list of objects as emit()'ed by the map() function
    // Make sure the object your return is of the same type as the ones emit()'ed
    //
    // Available functions: assert(), BinData(), DBPointer(), DBRef(), doassert(), emit(), gc()
    //                      HexData(), hex_md5(), isNumber(), isObject(), ISODate(), isString()
    //                      Map(), MD5(), NumberInt(), NumberLong(), ObjectId(), print()
    //                      printjson(), printjsononeline(), sleep(), Timestamp(), tojson()
    //                      tojsononeline(), tojsonObject(), UUID(), version()
    //
    // Available properties: args, MaxKey, MinKey

	//print("REDUCE IN","KEY", key);

	var result = {Icao: [], count: ""};

    var arr = [];
	var valsInitial = 0;

	//Guardem en array els valors dels Icaos de l'objecte values	
	for (i in values){
		//print("INPUT VALUES: Icao:", values[i].Icao, ".Count", values[i].count); //READ OK
	    arr[i]=values[i].Icao;
    	valsInitial++;
	  }
	  
	//Guardem en array out els Icaos no repetits
    var seen = {};
    var out = [];
    for (i in arr) {
        if (!(arr[i] in seen)) {
            out.push(arr[i]);
            seen[arr[i]] = true;
        }
    }

	var valsReduced=0; 
	//Guardem valors ICAO no repetits com array per retornar-los
	result = {Icao : out[], count: 1};

	//print("Registres duplicats eliminats: ", valsInitial-valsReduced);  // RESUKLT OK

	for (i in result){
		//print("OUTPUT VALUES: Icao:", result.Icao, ".Count", result.count);  //RESULT OK
	}

	return result;   //OK
};

// Variable for finalize
var __3tsoftwarelabs_finalize = function (key, reducedValue) {

    // Enter the JavaScript code for the finalize() function here.
    // 'reducedValue' is the result of the last reduce on 'key'
    // The return value will be the result of this map-reduce job for this 'key'
    //
    // Available functions: assert(), BinData(), DBPointer(), DBRef(), doassert(), emit(), gc()
    //                      HexData(), hex_md5(), isNumber(), isObject(), ISODate(), isString()
    //                      Map(), MD5(), NumberInt(), NumberLong(), ObjectId(), print()
    //                      printjson(), printjsononeline(), sleep(), Timestamp(), tojson()
    //                      tojsononeline(), tojsonObject(), UUID(), version()
    //
    // Available properties: args, MaxKey, MinKey

    print("Inici Finalize. Ciutat.", key);
    var arr = [];

	  
	  print("ValorsvalsInitial:",valsInitial);
 	  
    return Array.sum(arr);
};

db.runCommand({ 
    mapReduce: "adsb",
    map: __3tsoftwarelabs_map,
    reduce: __3tsoftwarelabs_reduce,
    finalize: __3tsoftwarelabs_finalize,
    out: { "inline" : 1},
    query: {  },
    sort: {  },
    inputDB: "uoc",
 });
