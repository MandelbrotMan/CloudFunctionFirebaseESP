const express = require('express');
//const passport = require('passport');
const app = express();
const parser = require('xml2json');
const keys = require('./Config/dev');
const firebase = require('firebase');
const NodeGeocoder = require('node-geocoder');
//const _ = require('lodash');
const config = {
    apiKey: "AIzaSyBaQnuxnK0f0eI-AxJkqWEUPXG6l1KuIYg",
    authDomain: "sales-automation-1507225972021.firebaseapp.com",
    databaseURL: "https://sales-automation-1507225972021.firebaseio.com",
    projectId: "sales-automation-1507225972021",
    storageBucket: "sales-automation-1507225972021.appspot.com",
    messagingSenderId: "850031720735"
};
firebase.initializeApp(config);
var firebaseDB = firebase.database();
var options = {
    provider: 'google',
    httpAdapter: 'https', // Default
    apiKey: 'AIzaSyBaQnuxnK0f0eI-AxJkqWEUPXG6l1KuIYg', // for Mapquest, OpenCage, Google Premier
    formatter: null         // 'gpx', 'string', ...
}
var geocoder = NodeGeocoder(options);

var date = new Date();
date.setDate(date.getDate() - 3);
var salesFromMWS = null; //array for data from MWS
var newSalesApproved = [];

var citiesWithCount = [];
var citiesNeedUpdating = [];
var mws = require('amazon-mws-node')({
        AmzSecretKey: keys.AmazonSecretKey,
        AWSAccessKeyId: keys.AmazonClientSecretID
        });
var schedule = require('node-schedule');

var j = schedule.scheduleJob('0 0 * * *', function(){
    mws({
        method: 'GET',
        base: 'mws.amazonservices.com',
        endpoint: '/Orders/2017-10-01',
        params: {
            'Action': 'ListOrders',
            'CreatedAfter': date.toISOString(),
            'MarketplaceId.Id.1': keys.MarketPlace_ID,
            'SellerId': keys.Seller_ID,
            'MWSAuthToken': 'MWS_AUTH_TOKEN',
            'Version': '2013-09-01'
        },
        callback: function (error, response, body) {
            let json = parser.toJson(body);
            let jsObject = JSON.parse(json);
            try{   
                   processSalesFromMWS(jsObject);
                   //newSalesApproved = getGeoForList(newSalesApproved);
                   sendOrdersToFB(newSalesApproved);   
                   sendToCityInFB(newSalesApproved);
                   getCitiesCount();
                  
    
                } catch(err){
                    console.log(err);
                }
            
            }
    });
    let citiesRef = firebaseDB.ref('Operations').push({
        success: 'true'
    });
})

const PORT = process.env.PORT || 5000;
app.listen(PORT);
function findRepeatingCustomer(OrdersInCity){
    for(let i = 1; i < ordersInCity.length; ++ i){
        let address1 = ordersInCity[i].AddressLine1 + "";
        let address2 =  ordersInCity[i-1].AddressLine1 + "";
        if(address1.toLowerCase() === address2.toLowerCase() && address1 !== null)
            firebaseDB.ref('RepeatingCustomer/'+OrdersInCity[i].PurchaseDate).update({
                PurchaseDate: OrdersInCity[i].PurchaseDate,
                BuyerName: OrdersInCity[i].BuyerName
            });
    }
}
function getCitiesCount(){
        let citiesRef = firebaseDB.ref('COOITC');
        citiesRef.once('value',function(snapshot){
            snapshot.forEach(function(childSnapshot){
                citiesWithCount.push(childSnapshot.val());
            });
        }).then((result) => {
            updateCityCountLocally();
            updateCOOITC(citiesNeedUpdating);
        });
}

function getGeoForList(Orders){
    for(let i = 0; i < Orders.length; ++ i){
        //Used for the unique id for entry in firebase
        try{
            geocoder.geocode(Order.AddressLine1 + " " + Order.City).then(
                function(res){
                    try{
                        Orders[i].Latitude = latNLon.latitude;
                        Orders[i].Longtiude = latNLon.longitude;
                    }catch(e){
                    }
            }) 
        }catch(e){
            console.log("Corrupted object at ", i, " : ", listOfOrders[i]);
        }
    }
    return Orders;
}
function processSalesFromMWS(jsObject){
    salesFromMWS = jsObject.ListOrdersResponse.ListOrdersResult.Orders.Order;
    var total = salesFromMWS.length;
    for(i = 0; i < total; ++i){
        if(salesFromMWS[i].OrderStatus == "Shipped"){  
            //The date and time are origiRepeatingCustomernally in a single String
            var times = salesFromMWS[i].PurchaseDate.split('T');
            var newSale = ({
                LatestShipDate:salesFromMWS[i].LatestShipDate,
                OrderType: salesFromMWS[i].OrderType,
                PurchaseDate: times[0],
                PurchaseTime: times[1],
                AmazonOrderId: salesFromMWS[i].AmazonOrderId,
                IsReplacementOrder: salesFromMWS[i].IsReplacementOrder,
                NumberOfItemsShipped: salesFromMWS[i].NumberOfItemsShipped,
                ShipServiceLevel: salesFromMWS[i].ShipServiceLevel,
                BuyerName: salesFromMWS[i].BuyerName,
                Amount: salesFromMWS[i].OrderTotal.Amount,
                ShipmentServiceLevelCategory: salesFromMWS[i].ShipmentServiceLevelCategory
            });
            try{
                newSale.AddressLine1 = salesFromMWS[i].ShippingAddress.AddressLine1
                newSale.City = salesFromMWS[i].ShippingAddress.City;
                newSale.CurrencyCode = salesFromMWS[i].OrderTotal.CurrencyCode
                newSale.PostalCode = salesFromMWS[i].ShippingAddress.PostalCode
                newSale.StateOrRegion =salesFromMWS[i].ShippingAddress.StateOrRegion
                newSale.IsPrime = salesFromMWS[i].IsPrime
                newSale.ShippingAddressName = salesFromMWS[i].ShippingAddress.Name
            }catch(e){
                console.log("there was a invalid property inside this obj");
            }
            newSalesApproved.push(newSale);
        }         
    } 
}
function sendOrdersToFB(listOfOrders){
    for(let i = 0; i < listOfOrders.length; ++ i){
        //Used for the unique id for entry in firebase
        try{
            let numericDateValue = new Date(listOfOrders[i].PurchaseDate).getTime();
            geocoder.geocode(listOfOrders[i].AddressLine1 + " " + listOfOrders[i].City).then(
                function(res){
                    listOfOrders[i].Latitude = res[0].latitude;
                    listOfOrders[i].Longtiude = res[0].longitude;
                    firebaseDB.ref('CustomerOrderAMZ/'+numericDateValue + listOfOrders[i].PurchaseTime)
                              .update(listOfOrders[i]);
            })
        }catch(e){
            console.log("SendOrdersToFB issue ", i, " : ", listOfOrders[i]);
        }
    }
}
function sendToCityInFB(listOfOrders){
    for(let i = 0; i < listOfOrders.length; ++i){
        let cityName = listOfOrders[i].City;
        cityName = cityName.toLowerCase();
        let intoCityRef = firebaseDB.ref('Cities/'+ cityName + '/' + listOfOrders[i].AmazonOrderId);
        intoCityRef.push(listOfOrders[i]);
    }
}

function updateCityCountLocally(){
    let organizedByCity = newSalesApproved.sort(function(a, b) { try {if( a.City > b.City ) return 1;
                                                                      if( a.City < b.City ) return -1;
                                                                      }catch(e){}
                                                                 return 0;})


    let currentCity = organizedByCity[0].City;
    currentCity = currentCity.toLowerCase();
    let newCity = null;
    //Go through all the new orders and count how many new orders are in each city
    for(let i = 1; i < organizedByCity.length; ++i){
        newCity = organizedByCity[i].City.toLowerCase();
        if(currentCity !== newCity){
            let position = null;
            //Now we look for the array position of the city 
            for(var j = 0; j < citiesWithCount.length && position === null; ++j){
                if(citiesWithCount[j].cityName === currentCity){
                    position = j;
                } 
            }
            citiesNeedUpdating.push(citiesWithCount[position]);
        }
        currentCity = newCity;
    }
}
//Taking all the orders already in firebase and organizing them into cities.
function updateCOOITC(citiesNeedUpdating){
    for(let i = 0; i < citiesNeedUpdating.length; ++i){
            let cityName = citiesNeedUpdating[i].cityName;
            let intoCityRef = firebaseDB.ref('Cities/'+ cityName);
            let totalOrders = 0;
            intoCityRef.once('value', function(grandChildSnapshot){
                grandChildSnapshot.forEach(function (lastKin){
                  ++totalOrders;
                });
                console.log(cityName, "updating to ", totalOrders);
                firebaseDB.ref('COOITC/'+cityName).update({
                    count: totalOrders
                })
            })
    }        
}
/////////////BULKY OPERATIONS    
function organizeIntoCitiesOnFB(){
    let listOfOrdersRef = firebaseDB.ref('CustomerOrderAMZ');
    listOfOrdersRef.once('value', function(snapshot){
        snapshot.forEach(function(childSnapshot) {
            let amazonOrderObj = childSnapshot.val();
            let city = amazonOrderObj.City + "";
            city = city.toLowerCase();
            firebaseDB.ref('Cities/'+ city +'/' + amazonOrderObj.AmazonOrderId)
                      .update(amazonOrderObj);
        })
    })
}
function countNumSales(){
    let listOfCitiesRef = firebaseDB.ref('Cities');
    listOfCitiesRef.once('value', function(snapshot){
        let i = 0;
        snapshot.forEach(function(childSnapshot) {
            let intoCityRef = firebaseDB.ref('Cities/'+ childSnapshot.key);
            intoCityRef.once('value', function(grandChildSnapshot){
                listOfRepeatingCustomers = [];
                ordersInCity = [];
                grandChildSnapshot.forEach(function (lastKin){
                    ordersInCity.push(lastKin.val());
                    ++i;
                });
                firebaseDB.ref('COOITIC/')
                ordersInCity.sort(function(a,b) {
                    let address1 = a.AddressLine1 + "";
                    let address2 = b.AddressLine1 + "";
                    return address1.localeCompare(address2);
                });
                findRepeatingCustomer(ordersInCity);
                
                i = 0;
            })

        });
    })
}
//Iterating through all orders on Firebase and updating the ones without geolocation
function getOrderGeoLocations(){
    //Upper and lower bounds are used since at max 50 api calls maybe used for Node geocoder
    let UpperBound = 850;
    let LowerBound= 800;
    let basicStart = 0;
    let listOfCitiesRef = firebaseDB.ref('Cities');
    listOfCitiesRef.once('value', function(snapshot){
        let i = 0;
        snapshot.forEach(function(childSnapshot) {
            let cityName = childSnapshot.key;
            let intoCityRef = firebaseDB.ref('Cities/'+ cityName);
            intoCityRef.once('value', function(grandChildSnapshot){
                grandChildSnapshot.forEach(function (lastKin){
                    if(basicStart < UpperBound && basicStart >= LowerBound)
                        updateGeoCode(lastKin.val(), cityName);
                    ++basicStart;
                });
                i = 0;
            })
        }); 
    })
}
function updateAddressGeolocation(Order, cityName){
    geocoder.geocode(Order.AddressLine1 + " " + cityName).then(
        function(res){
            try{
                let path = 'Cities/'+ cityName +"/"+Order.AmazonOrderId+"/";
                console.log("trying to update to path ", path);
                firebaseDB.ref(path).update({
                    Latitude: res[0].latitude,
                    Longitude: res[0].longitude
                })
            }catch(e){
            }
        }) 
}
