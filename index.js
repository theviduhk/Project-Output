import { BigQuery } from "@google-cloud/bigquery";
import fetch from "node-fetch";

// =========================
// CONFIG
// =========================

// 👉 Google service account JSON path
// (download from Google Cloud Console)
const bigquery = new BigQuery({
  keyFilename: "./service-account.json",
  projectId: "trax-ortal-prod",
});

const FIREBASE_URL =
  "https://projectgap-4b7d9-default-rtdb.firebaseio.com/project-gap.json";

// =========================
// MAIN LOOP
// =========================
async function mainLoop() {
  try {
    console.log("Fetching from BigQuery...");

    const rows = await runQuery();
    const formatted = processResults(rows);

    await updateFirebase(formatted);

    console.log("✅ Updated:", new Date().toLocaleTimeString());
  } catch (err) {
    console.error("❌ Error:", err.message);
  }
}

// =========================
// BIGQUERY QUERY
// =========================
async function runQuery() {
  const query = `
    SELECT
      CONCAT(project_name, ' | ', task_name, ' | ', center) AS metric,
      SUM(count) AS value
    FROM \`trax-retail.backoffice.560_project_outflow\`
    WHERE DATE(event_timestamp) = CURRENT_DATE()
    GROUP BY 1
    ORDER BY value DESC
  `;

  const [rows] = await bigquery.query({
    query,
    location: "US",
  });

  return rows;
}

// =========================
// PROCESS DATA
// =========================
function processResults(rows) {
  return rows.map((row) => {
    const [project, task, center] =
      (row.metric || "N/A | N/A | N/A").split(" | ");

    return {
      project,
      task,
      center,
      value: Number(row.value || 0),
    };
  });
}

// =========================
// FIREBASE UPDATE
// =========================
async function updateFirebase(data) {
  const res = await fetch(FIREBASE_URL, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      lastUpdated: new Date().toISOString(),
      data,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Firebase error: ${text}`);
  }
}

// =========================
// LOOP (30 sec)
// =========================
async function startLoop() {
  while (true) {
    await mainLoop();
    await new Promise((r) => setTimeout(r, 30000));
  }
}

startLoop();
