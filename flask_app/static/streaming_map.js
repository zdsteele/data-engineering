// Initialize the map
// Initialize the map
var map = L.map('map')
    .setView([20, 0], 2); // Set default view coordinates and zoom level

//document.addEventListener('DOMContentLoaded', function() {
//  var map = L.map('map', {
//    center: [0, 0],
//    zoom: 2,
//    minZoom: 2,
//    maxBounds: [[-90, -180], [90, 180]]
//  });
//
//  L.tileLayer('https://api.mapbox.com/styles/v1/zdsteele/cm644yejt00em01s2b15nezld/tiles/256/{z}/{x}/{y}?access_token=pk.eyJ1IjoiemRzdGVlbGUiLCJhIjoiY202NDNjanZzMWNmeTJ2cHk5NmhmeGJkcyJ9.LQOFJFUkr6h7Lalj95r4Rg', {
//    maxZoom: 19,
//    attribution: '© <a href="https://www.mapbox.com/about/maps/">Mapbox</a>'
//  }).addTo(map);
//});

// Add Mapbox tiles
L.tileLayer(`https://api.mapbox.com/styles/v1/mapbox/dark-v10/tiles/{z}/{x}/{y}?access_token=${MAPBOX_ACCESS_TOKEN}`, {
    maxZoom: 18,
    attribution: '© <a href="https://www.mapbox.com/">Mapbox</a>',
    noWrap: true // Prevent horizontal map repetition
}).addTo(map);

// EventSource to listen for streamed data
const eventSource = new EventSource('/stream');

eventSource.onmessage = function (event) {
    try {
        // Parse streamed data
        const parsedData = JSON.parse(event.data);

        // Extract necessary fields
        const {
            Passenger_ID,
            First_Name,
            Last_Name,
            Latitude,
            Longitude,
            Flight_Status
        } = parsedData;

        // Create a popup message for the circle marker
        const popupContent = `
            <b>${First_Name} ${Last_Name}</b><br>
            Passenger ID: ${Passenger_ID}<br>
            Flight Status: ${Flight_Status}
        `;

        // Determine circle color based on `Flight_Status`
        let color = "blue"; // Default
        if (Flight_Status === "Delayed") {
            color = "orange";
        } else if (Flight_Status === "Cancelled") {
            color = "red";
        } else if (Flight_Status === "On Time") {
            color = "green";
        }

        // Add a circle marker to the map
        L.circleMarker([Latitude, Longitude], {
            color: color,
            radius: 10, // Adjust circle size
            fillOpacity: 0.7
        })
            .addTo(map)
            .bindPopup(popupContent);

        console.log(`Added marker for: ${First_Name} ${Last_Name}`);
    } catch (error) {
        console.error("Error processing event data:", error.message, "Raw data:", event.data);
    }
};
