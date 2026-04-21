import fetch from "node-fetch";

const GRAFANA_QUERY_URL =
  "https://monitor-public.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-ortal-prod/queries";

// ✅ session cookie value only
const SESSION = "c8eaf4ebc900f42829a1e55664c4fb73";

const FIREBASE_URL =
  "https://projectgap-4b7d9-default-rtdb.firebaseio.com/project-gap.json";

// -----------------------------
// 🔥 SAFE JSON PARSER
// -----------------------------
async function safeJson(res) {
  const text = await res.text();

  try {
    return JSON.parse(text);
  } catch (e) {
    console.log("❌ RAW RESPONSE:\n", text);
    throw new Error("Non-JSON response (likely 403 or HTML)");
  }
}

// -----------------------------
// 🔁 MAIN LOOP
// -----------------------------
async function mainLoop() {
  try {
    console.log("Fetching...");

    const job = await runQuery();
    const results = await getQueryResults(job);
    const formatted = processResults(results);

    await updateFirebase(formatted);

    console.log("✅ Updated:", new Date().toLocaleTimeString());
  } catch (err) {
    console.error("❌ Error:", err.message);
  }
}

// -----------------------------
// ▶️ RUN QUERY
// -----------------------------
async function runQuery() {
  const body = {
    query: `
      #standardSQL
      SELECT
        CONCAT(project_name, ' | ', task_name, ' | ', center) AS metric,
        SUM(count) AS value
      FROM \`trax-retail.backoffice.560_project_outflow\`
      WHERE DATE(event_timestamp) = CURRENT_DATE()
      GROUP BY 1
      ORDER BY value DESC
    `,
    useLegacySql: false,
  };

  const res = await fetch(GRAFANA_QUERY_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json",

      // ✅ IMPORTANT FIX
      "Cookie": `grafana_session=${SESSION}`,

      // sometimes required
      "X-Grafana-Org-Id": "1",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    console.log("❌ STATUS:", res.status);
    console.log(await res.text());
    throw new Error("Query request failed (check auth / session)");
  }

  const data = await safeJson(res);

  return {
    jobId: data.jobReference.jobId,
    location: data.jobReference.location,
  };
}

// -----------------------------
// ▶️ POLL RESULTS
// -----------------------------
async function getQueryResults(job) {
  const url = `${GRAFANA_QUERY_URL}/${job.jobId}?location=${job.location}`;

  for (let i = 0; i < 10; i++) {
    const res = await fetch(url, {
      headers: {
        "Accept": "application/json",
        "Cookie": `grafana_session=${SESSION}`,
      },
    });

    if (!res.ok) {
      console.log("❌ POLLING STATUS:", res.status);
      console.log(await res.text());
      throw new Error("Polling failed (auth issue likely)");
    }

    const json = await safeJson(res);

    if (json.jobComplete) return json;

    await new Promise((r) => setTimeout(r, 1500));
  }

  throw new Error("Timeout waiting for query result");
}

// -----------------------------
// ▶️ PROCESS DATA
// -----------------------------
function processResults(result) {
  if (!result.rows) return [];

  const fields = result.schema.fields.map((f) => f.name);

  return result.rows.map((row) => {
    const obj = {};
    row.f.forEach((col, i) => (obj[fields[i]] = col.v));

    const [project, task, center] =
      (obj.metric || "N/A | N/A | N/A").split(" | ");

    return {
      project,
      task,
      center,
      value: Number(obj.value || 0),
    };
  });
}

// -----------------------------
// 🔥 FIREBASE UPDATE
// -----------------------------
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

// -----------------------------
// 🔁 LOOP
// -----------------------------
async function startLoop() {
  while (true) {
    await mainLoop();

    // 30 sec delay
    await new Promise((r) => setTimeout(r, 30000));
  }
}

startLoop();
