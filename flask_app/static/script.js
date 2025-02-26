const eventSource = new EventSource('/stream');

eventSource.onmessage = function (event) {
    const dataList = document.getElementById('data-list');

    try {
        // Parse the raw JSON data
        const parsedData = JSON.parse(event.data);
        console.log("Parsed data:", parsedData);

        // Check if the expected structure exists
        if (!parsedData || typeof parsedData !== 'object') {
            console.error("Invalid data format:", parsedData);
            return;
        }

        // Access fields directly from the parsed data
        const {
            Passenger_ID,
            First_Name,
            Last_Name,
            Country_Code,
            Flight_Status,
            Latitude,
            Longitude
        } = parsedData;

        // Create a new list item
        const newItem = document.createElement('li');
        newItem.textContent = `Passenger ID: ${Passenger_ID}, Name: ${First_Name} ${Last_Name}, Flight Status: ${Flight_Status}`;

        // Apply styles for specific conditions
        if (Flight_Status === "Delayed") {
            newItem.style.color = "red";
        } else if (Flight_Status === "Cancelled") {
            newItem.style.color = "orange";
        } else {
            newItem.style.color = "green"; // For 'On Time' or other statuses
        }

        // Append the new item to the list
        dataList.appendChild(newItem);
    } catch (error) {
        console.error("Error processing event data:", error.message, "Raw data:", event.data);
    }
};
