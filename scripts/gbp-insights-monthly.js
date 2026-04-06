import "dotenv/config";
import { DateTime } from "luxon";
import fs from "fs";
import Papa from "papaparse";

// -------------------- ENV --------------------
const ENV = {
  GBP_CLIENT_ID:     (process.env.GBP_CLIENT_ID     || "").trim(),
  GBP_CLIENT_SECRET: (process.env.GBP_CLIENT_SECRET || "").trim(),
  GBP_REFRESH_TOKEN: (process.env.GBP_REFRESH_TOKEN || "").trim(),
  GBP_ACCOUNT_ID:    (process.env.GBP_ACCOUNT_ID    || "").trim(),

  MAKE_INSIGHTS_WEBHOOK_URL_MONTHLY: (process.env.MAKE_INSIGHTS_WEBHOOK_URL_MONTHLY || "").trim(),

  // Sequentiell (1) um 429 zu vermeiden
  CONCURRENCY: Number(process.env.CONCURRENCY || "1"),
};

function mustEnv(key) {
  if (!ENV[key]) throw new Error(`Missing env: ${key}`);
  return ENV[key];
}

// -------------------- TIME RANGE --------------------
const TZ = "Europe/Berlin";

function getPreviousMonthRange() {
  const now  = DateTime.now().setZone(TZ);
  const prev = now.minus({ months: 1 }).startOf("month");
  return {
    start: prev,
    end:   prev.endOf("month"),
    label: prev.toFormat("yyyy-MM"),
  };
}

const { start, end, label } = getPreviousMonthRange();

// -------------------- Metrics --------------------
const METRICS = [
  "BUSINESS_IMPRESSIONS_DESKTOP_SEARCH",
  "BUSINESS_IMPRESSIONS_MOBILE_SEARCH",
  "BUSINESS_IMPRESSIONS_DESKTOP_MAPS",
  "BUSINESS_IMPRESSIONS_MOBILE_MAPS",
  "WEBSITE_CLICKS",
  "CALL_CLICKS",
  "BUSINESS_DIRECTION_REQUESTS",
];

// -------------------- HTTP Helpers --------------------
async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function requestWithRetry(url, options = {}, { retries = 5, baseBackoffMs = 2000 } = {}) {
  let lastErr;
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, options);

      // 429: warte länger und retry
      if (res.status === 429) {
        const wait = baseBackoffMs * Math.pow(2, i);
        console.warn(`    429 – warte ${wait}ms …`);
        await sleep(wait);
        lastErr = new Error(`HTTP 429 Too Many Requests`);
        continue;
      }

      if ([500, 502, 503, 504].includes(res.status)) {
        lastErr = new Error(`HTTP ${res.status} ${res.statusText}`);
        await sleep(baseBackoffMs * Math.pow(2, i));
        continue;
      }

      return res;
    } catch (e) {
      lastErr = e;
      await sleep(baseBackoffMs * Math.pow(2, i));
    }
  }
  throw lastErr || new Error("requestWithRetry failed");
}

async function getJson(url, { headers = {}, params = null } = {}) {
  const u = new URL(url);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && String(v).length > 0)
        u.searchParams.set(k, String(v));
    }
  }
  const res = await requestWithRetry(u.toString(), { headers, method: "GET" });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`GET ${u.toString()} -> ${res.status}: ${txt}`);
  }
  return res.json();
}

// -------------------- Google OAuth --------------------
async function getAccessToken() {
  const body = new URLSearchParams({
    client_id:     ENV.GBP_CLIENT_ID,
    client_secret: ENV.GBP_CLIENT_SECRET,
    refresh_token: ENV.GBP_REFRESH_TOKEN,
    grant_type:    "refresh_token",
  });

  const res = await requestWithRetry("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  const txt = await res.text();
  if (!res.ok) throw new Error(`Token error ${res.status}: ${txt}`);
  const j = JSON.parse(txt);
  if (!j.access_token) throw new Error(`No access_token: ${txt}`);
  return j.access_token;
}

// -------------------- GBP: Locations --------------------
async function listLocations(accessToken, accountId) {
  const out = [];
  let pageToken = "";

  do {
    const j = await getJson(
      `https://mybusinessbusinessinformation.googleapis.com/v1/accounts/${accountId}/locations`,
      {
        headers: { Authorization: `Bearer ${accessToken}` },
        params: {
          pageSize:  "100",
          readMask:  "name,title,storeCode",
          orderBy:   "storeCode",
          pageToken,
        },
      }
    );
    out.push(...(j.locations || []));
    pageToken = j.nextPageToken || "";
  } while (pageToken);

  return out;
}

// -------------------- GBP: Performance API --------------------
async function fetchMetricSum(accessToken, locationId, metric, startDt, endDt) {
  const j = await getJson(
    `https://businessprofileperformance.googleapis.com/v1/locations/${locationId}:getDailyMetricsTimeSeries`,
    {
      headers: { Authorization: `Bearer ${accessToken}` },
      params: {
        dailyMetric:                   metric,
        "dailyRange.startDate.year":   String(startDt.year),
        "dailyRange.startDate.month":  String(startDt.month),
        "dailyRange.startDate.day":    String(startDt.day),
        "dailyRange.endDate.year":     String(endDt.year),
        "dailyRange.endDate.month":    String(endDt.month),
        "dailyRange.endDate.day":      String(endDt.day),
      },
    }
  );

  const values = j?.timeSeries?.datedValues || [];
  return values.reduce((sum, d) => sum + (Number(d.value) || 0), 0);
}

async function fetchInsightsForLocation(accessToken, locationId, startDt, endDt) {
  const results = {};

  for (const metric of METRICS) {
    try {
      results[metric] = await fetchMetricSum(accessToken, locationId, metric, startDt, endDt);
    } catch (e) {
      // 403 = keine Berechtigung für diese Metric/Location → 0, nicht crashen
      if (e.message.includes("403")) {
        results[metric] = 0;
      } else {
        throw e; // andere Fehler weitergeben
      }
    }
    await sleep(600); // 600ms zwischen Metrics → ~4s pro Location
  }

  const views_search = results["BUSINESS_IMPRESSIONS_DESKTOP_SEARCH"]
                     + results["BUSINESS_IMPRESSIONS_MOBILE_SEARCH"];
  const views_maps   = results["BUSINESS_IMPRESSIONS_DESKTOP_MAPS"]
                     + results["BUSINESS_IMPRESSIONS_MOBILE_MAPS"];
  const views        = views_search + views_maps;

  const actions_website            = results["WEBSITE_CLICKS"];
  const actions_phone              = results["CALL_CLICKS"];
  const actions_driving_directions = results["BUSINESS_DIRECTION_REQUESTS"];
  const actions                    = actions_website + actions_phone + actions_driving_directions;

  return {
    views,
    actions,
    views_search,
    views_maps,
    actions_website,
    actions_phone,
    actions_driving_directions,
  };
}

// -------------------- Concurrency pool --------------------
async function asyncPool(limit, items, fn) {
  const ret = [];
  const executing = [];
  for (const item of items) {
    const p = Promise.resolve().then(() => fn(item));
    ret.push(p);
    if (limit <= items.length) {
      const e = p.then(() => executing.splice(executing.indexOf(e), 1));
      executing.push(e);
      if (executing.length >= limit) await Promise.race(executing);
    }
  }
  return Promise.all(ret);
}

// -------------------- MAIN --------------------
async function main() {
  mustEnv("GBP_CLIENT_ID");
  mustEnv("GBP_CLIENT_SECRET");
  mustEnv("GBP_REFRESH_TOKEN");
  mustEnv("GBP_ACCOUNT_ID");

  console.log(`TZ:      ${TZ}`);
  console.log(`Monat:   ${label} (${start.toISODate()} → ${end.toISODate()})`);
  console.log(`Account: ${ENV.GBP_ACCOUNT_ID}`);
  console.log(`Concurrency: ${ENV.CONCURRENCY}`);

  console.log("\n1) Access token …");
  const accessToken = await getAccessToken();
  console.log("✓ token ok");

  console.log("\n2) Locations …");
  const locations = await listLocations(accessToken, ENV.GBP_ACCOUNT_ID);
  console.log(`✓ ${locations.length} locations`);

  const rows = [];
  const skipped = [];

  console.log("\n3) Insights (Performance API) …");

  await asyncPool(ENV.CONCURRENCY, locations, async (loc) => {
    const locationId    = (loc.name || "").split("/").pop();
    const storeCode     = (loc.storeCode || "").toString().trim();
    const locationTitle = (loc.title || "").trim();
    const label_loc     = storeCode || locationTitle || locationId;

    if (!locationId) return;

    let insights;
    try {
      insights = await fetchInsightsForLocation(accessToken, locationId, start, end);
    } catch (e) {
      console.warn(`  ⚠ ${label_loc}: ${e.message}`);
      skipped.push(label_loc);
      return;
    }

    console.log(`  ✓ ${label_loc}: views=${insights.views} actions=${insights.actions}`);

    rows.push({
      Store:                       storeCode || null,
      month:                       label,
      views:                       insights.views,
      actions:                     insights.actions,
      views_search:                insights.views_search,
      views_maps:                  insights.views_maps,
      actions_website:             insights.actions_website,
      actions_phone:               insights.actions_phone,
      actions_driving_directions:  insights.actions_driving_directions,
    });
  });

  rows.sort((a, b) => (a.Store || "").localeCompare(b.Store || ""));

  console.log(`\n✓ Locations mit Daten: ${rows.length}`);
  if (skipped.length) console.log(`⚠ Übersprungen (${skipped.length}): ${skipped.join(", ")}`);

  // -------------------- CSV Export --------------------
  const filename = `gbp-insights-${label}.csv`;
  const csv = Papa.unparse(rows);
  fs.writeFileSync(filename, "\uFEFF" + csv, "utf-8");
  console.log(`\n📄 CSV gespeichert: ${filename}`);

  // -------------------- Make Webhook (optional) --------------------
  if (ENV.MAKE_INSIGHTS_WEBHOOK_URL_MONTHLY) {
    const res = await requestWithRetry(ENV.MAKE_INSIGHTS_WEBHOOK_URL_MONTHLY, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type:      "gbp_insights_monthly",
        month:     label,
        dateFrom:  start.toISODate(),
        dateTo:    end.toISODate(),
        row_count: rows.length,
        skipped,
        rows,
      }),
    });
    const txt = await res.text().catch(() => "");
    console.log(`🚀 Make Webhook → ${res.status} ${txt.slice(0, 100)}`);
  } else {
    console.log("ℹ️  MAKE_INSIGHTS_WEBHOOK_URL_MONTHLY nicht gesetzt – Webhook übersprungen");
  }

  console.log("\n✅ Fertig");
}

main().catch((e) => {
  console.error("\n❌ ERROR:", e?.message || e);
  process.exit(1);
});
