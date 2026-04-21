import fetch from "node-fetch";

const GRAFANA_QUERY_URL = "https://monitor-public.trax-cloud.com/api/datasources/proxy/133/bigquery/v2/projects/trax-ortal-prod/queries";

const USERNAME = "gss.kurunegala@gssintl.biz";
const PASSWORD = "Gssk@2021";

const FIREBASE_URL = "https://projectgap-4b7d9-default-rtdb.firebaseio.com/project-gap.json";

const headers = {
  "Authorization": "Basic " + Buffer.from(`${USERNAME}:${PASSWORD}`).toString("base64"),
  "Content-Type": "application/json"
};

async function mainLoop() {
  try {
    console.log("Fetching...");

    const job = await runQuery();
    const results = await getQueryResults(job);
    const formatted = processResults(results);

    await updateFirebase(formatted);

    console.log("Updated:", new Date().toLocaleTimeString());

  } catch (err) {
    console.error("Error:", err.message);
  }
}

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
    useLegacySql: false
  };

  const res = await fetch(GRAFANA_QUERY_URL, {
    method: "POST",
    headers,
    body: JSON.stringify(body)
  });

  const data = await res.json();

  return {
    jobId: data.jobReference.jobId,
    location: data.jobReference.location
  };
}

async function getQueryResults(job) {
  const url = `${GRAFANA_QUERY_URL}/${job.jobId}?location=${job.location}`;

  for (let i = 0; i < 5; i++) {
    const res = await fetch(url, { headers });
    const json = await res.json();

    if (json.jobComplete) return json;

    await new Promise(r => setTimeout(r, 1000));
  }

  throw new Error("Timeout");
}

function processResults(result) {
  if (!result.rows) return [];

  const fields = result.schema.fields.map(f => f.name);

  return result.rows.map(row => {
    const obj = {};
    row.f.forEach((col, i) => obj[fields[i]] = col.v);

    const [project, task, center] = (obj.metric || "N/A | N/A | N/A").split(" | ");

    return {
      project,
      task,
      center,
      value: Number(obj.value || 0)
    };
  });
}

async function updateFirebase(data) {
  await fetch(FIREBASE_URL, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      lastUpdated: new Date().toISOString(),
      data: data
    })
  });
}

// 🔁 LOOP EVERY 1 SECOND (safe async loop)
async function startLoop() {
  while (true) {
    await mainLoop();
    await new Promise(r => setTimeout(r, 1000)); // 1 second delay
  }
}

startLoop();
