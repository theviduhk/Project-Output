import fetch from "node-fetch";

const GRAFANA_BASE_URL = "https://monitor-public.trax-cloud.com";
const GRAFANA_LOGIN_URL = `${GRAFANA_BASE_URL}/login`;
const GRAFANA_QUERY_URL = `${GRAFANA_BASE_URL}/api/datasources/proxy/133/bigquery/v2/projects/trax-ortal-prod/queries`;
const FIREBASE_URL = "https://projectgap-4b7d9-default-rtdb.firebaseio.com/project-gap.json";

const USERNAME = "gss.kurunegala@gssintl.biz";
const PASSWORD = "Gssk@2021";

// This will hold the current session cookie in memory, auto-refreshed on login
let currentSession = null;

// ✅ SAFE JSON
async function safeJson(res) {
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`❌ Non-JSON response:\n${text.substring(0, 200)}`);
  }
}

// 🔑 LOGIN TO GRAFANA AND EXTRACT SESSION COOKIE
async function grafanaLogin() {
  console.log("🔑 Logging in to Grafana...");

  const res = await fetch(GRAFANA_LOGIN_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      user: USERNAME,
      password: PASSWORD
    }),
    redirect: "manual" // don't auto-follow redirects, we just need headers
  });

  // node-fetch v2/v3 both expose raw headers via res.headers.raw()
  const rawSetCookie = res.headers.raw()["set-cookie"];

  if (!rawSetCookie || rawSetCookie.length === 0) {
    const text = await res.text();
    throw new Error(`❌ Login failed, no Set-Cookie header returned. Status: ${res.status}\n${text.substring(0, 300)}`);
  }

  // Find the grafana_session cookie among possibly multiple Set-Cookie headers
  const sessionCookieStr = rawSetCookie.find(c => c.startsWith("grafana_session="));

  if (!sessionCookieStr) {
    throw new Error(`❌ grafana_session cookie not found in response:\n${rawSetCookie.join("\n")}`);
  }

  // Extract just "grafana_session=xxxx" part (strip attributes like Path=, HttpOnly, etc.)
  const sessionCookie = sessionCookieStr.split(";")[0];

  currentSession = sessionCookie;
  console.log("✅ Grafana login success, session acquired.");
  return currentSession;
}

// 🔁 Ensure we have a valid session, logging in if needed
async function ensureSession() {
  if (!currentSession) {
    await grafanaLogin();
  }
  return currentSession;
}

// 🔁 MAIN LOOP
async function mainLoop() {
  try {
    console.log("Fetching...");
    const job = await runQueryWithAuth();
    const results = await getQueryResultsWithAuth(job);
    const formatted = processResults(results);
    await updateFirebase(formatted);
    console.log("✅ Updated:", new Date().toLocaleTimeString());
  } catch (err) {
    console.error("❌ Error:", err.message);
  }
}

// Wrapper that retries once with a fresh login if session expired (401/403)
async function fetchWithSessionRetry(url, options) {
  await ensureSession();

  const doFetch = () =>
    fetch(url, {
      ...options,
      headers: {
        ...(options.headers || {}),
        Cookie: currentSession
      }
    });

  let res = await doFetch();

  if (res.status === 401 || res.status === 403) {
    console.log("⚠️ Session expired, re-logging in...");
    await grafanaLogin();
    res = await doFetch();
  }

  return res;
}

// ▶️ RUN QUERY
async function runQueryWithAuth() {
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

  const res = await fetchWithSessionRetry(GRAFANA_QUERY_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  const data = await safeJson(res);
  return {
    jobId: data.jobReference.jobId,
    location: data.jobReference.location
  };
}

// ▶️ GET RESULTS
async function getQueryResultsWithAuth(job) {
  const url = `${GRAFANA_QUERY_URL}/${job.jobId}?location=${job.location}`;
  for (let i = 0; i < 10; i++) {
    const res = await fetchWithSessionRetry(url, { method: "GET" });
    const json = await safeJson(res);
    if (json.jobComplete) return json;
    await new Promise(r => setTimeout(r, 1500));
  }
  throw new Error("Timeout");
}

// ▶️ PROCESS
function processResults(result) {
  if (!result.rows) return [];
  const fields = result.schema.fields.map(f => f.name);
  return result.rows.map(row => {
    const obj = {};
    row.f.forEach((col, i) => obj[fields[i]] = col.v);
    const [project, task, center] =
      (obj.metric || "N/A | N/A | N/A").split(" | ");
    return {
      project,
      task,
      center,
      value: Number(obj.value || 0)
    };
  });
}

// ▶️ FIREBASE
async function updateFirebase(data) {
  const res = await fetch(FIREBASE_URL, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      lastUpdated: new Date().toISOString(),
      data
    })
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Firebase error: ${text}`);
  }
}

// 🔁 LOOP
async function startLoop() {
  while (true) {
    await mainLoop();
    await new Promise(r => setTimeout(r, 10000)); // 10 sec
  }
}

startLoop();
