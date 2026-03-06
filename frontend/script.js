async function performSearch() {
  const query = document.getElementById("searchInput").value;

  const resultsDiv = document.getElementById("results");

  resultsDiv.innerHTML = "Searching...";

  const response = await fetch(
    "http://localhost:3000/search?q=" + encodeURIComponent(query),
  );

  const results = await response.json();

  resultsDiv.innerHTML = "";

  if (results.length === 0) {
    resultsDiv.innerHTML = "No results found";
    return;
  }

  results.forEach((result) => {
    const div = document.createElement("div");
    div.className = "result";

    div.innerHTML = `
            <a href="${result.url}" target="_blank">${result.title}</a>
            <p>${result.url}</p>
            <small>Score: ${result.score}</small>
        `;

    resultsDiv.appendChild(div);
  });
}
