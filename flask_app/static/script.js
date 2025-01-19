const eventSource = new EventSource('/stream');

eventSource.onmessage = function(event) {
    const dataList = document.getElementById('data-list');

    // Parse the raw JSON data
    const parsedData = JSON.parse(event.data);
    console.log("Parsed data:", parsedData);

    // Parse the nested 'message' field
    const innerData = JSON.parse(parsedData.message);
    console.log("Inner data:", innerData);

    // Extract the id and name
    const id = innerData.id;
    const name = innerData.name;

    // Create a new list item
    const newItem = document.createElement('li');
    newItem.textContent = `ID: ${id}, Name: ${name}`;

    // Apply a different color for odd IDs
    if (id % 2 !== 0) {
        newItem.style.color = "red"; // Odd IDs
    } else {
        newItem.style.color = "blue"; // Even IDs
    }

    // Append the new item to the list
    dataList.appendChild(newItem);
};
