// Initial data setup for the graph
var flightStatusData = [
    { x: ["On Time", "Cancelled", "Delayed"], y: [0, 0, 0], type: "bar" },
];

var layout = {
    title: "Flight Status Updates",
    xaxis: { title: "Flight Status" },
    yaxis: { title: "Count" },
};

// Render the initial graph
Plotly.newPlot("graph", flightStatusData, layout);

// Real-time updates using EventSource
const eventSource = new EventSource('/stream');

// Track flight status counts
let statusCounts = { "On Time": 0, "Cancelled": 0, "Delayed": 0 };

eventSource.onmessage = function (event) {
    // Parse the received data
    const parsedData = JSON.parse(event.data);
    console.log("Graph received:", parsedData);

    // Extract the Flight Status field
    const flightStatus = parsedData.Flight_Status || "Unknown";

    // Increment the count for the received status
    if (statusCounts.hasOwnProperty(flightStatus)) {
        statusCounts[flightStatus]++;
    }

    // Update the graph with the new data
    flightStatusData[0].y = [
        statusCounts["On Time"],
        statusCounts["Cancelled"],
        statusCounts["Delayed"],
    ];

    Plotly.react("graph", flightStatusData, layout);
};
