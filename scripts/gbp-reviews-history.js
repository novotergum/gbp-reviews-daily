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

  MAKE_REVIEWS_WEBHOOK_URL_MONTHLY: (process.env.MAKE_REVIEWS_WEBHOOK_URL_MONTHLY || "").trim(),

  CONCURRENCY: Number(process.env.CONCURRENCY || "5"),
};

function mustEnv(key) {
  if (!ENV[key]) throw new Error(`Missing env: ${key}`);
  return ENV[key];
}

// -------------------- TIME RANGE --------------------
// Letzte 12 Monate bis einschließlich letzten Sonntag
const TZ = "Europe/Berlin";

function getLast12MonthsRange() {
  const today = DateTime.now().setZone(TZ).startOf("day");

  const weekday = today.weekday; // 1=Mo … 7=So
  const lastMonday = today.minus({ days: weekday - 1 });
  const end = lastMonday.minus({ days: 1 }); // letzter Sonntag

  const start = end.minus({ years: 1 }).plus({ days: 1 });

  return { start, end };
}

const { start, end } = getLast12MonthsRange();

// -------------------- HTTP Helpers --------------------
async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function requestWithRetry(url, options = {}, { retries = 4, baseBackoffMs = 800 } = {}) {
  let lastErr;
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, options);
      if ([429, 500, 502, 503, 504].includes(res.status)) {
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

// -------------------- GBP: Reviews --------------------
function starRatingToInt(star) {
  if (!star) return null;
  const map = { ONE: 1, TWO: 2, THREE: 3, FOUR: 4, FIVE: 5 };
  return map[String(star).toUpperCase()] ?? null;
}

function cleanComment(text) {
  let t = (text || "").trim();
  for (const m of ["(Translated by Google)", "(Übersetzt von Google)"]) {
    const idx = t.indexOf(m);
    if (idx !== -1) t = t.slice(0, idx).trimEnd();
  }
  return t;
}

async function listReviewsForLocation(accessToken, accountId, locationId, startDt, endDt) {
  const out = [];
  let pageToken = "";
  let pagesBelowCutoff = 0;

  do {
    const j = await getJson(
      `https://mybusiness.googleapis.com/v4/accounts/${accountId}/locations/${locationId}/reviews`,
      {
        headers: { Authorization: `Bearer ${accessToken}` },
        params: {
          pageSize: "50",
          orderBy:  "updateTime desc",
          pageToken,
        },
      }
    );

    const reviews = j.reviews || [];
    if (!reviews.length) break;

    let anyGeStart = false;

    for (const r of reviews) {
      if (!r.createTime) continue;
      const dt = DateTime.fromISO(r.createTime, { setZone: true }).setZone(TZ);

      if (dt >= startDt) anyGeStart = true;
      if (dt >= startDt && dt <= endDt) out.push(r);
    }

    if (!anyGeStart) pagesBelowCutoff++;
    else pagesBelowCutoff = 0;

    if (pagesBelowCutoff >= 2) break;

    pageToken = j.nextPageToken || "";
    if (pageToken) await sleep(120);
  } while (pageToken);

  return out;
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
  console.log(`Range:   ${start.toISODate()} → ${end.toISODate()}`);
  console.log(`Account: ${ENV.GBP_ACCOUNT_ID}`);

  console.log("\n1) Access token …");
  const accessToken = await getAccessToken();
  console.log("✓ token ok");

  console.log("\n2) Locations …");
  const locations = await listLocations(accessToken, ENV.GBP_ACCOUNT_ID);
  console.log(`✓ ${locations.length} locations`);

  const rows = [];

  console.log("\n3) Reviews …");

  await asyncPool(ENV.CONCURRENCY, locations, async (loc) => {
    const locationId    = (loc.name || "").split("/").pop();
    const storeCode     = (loc.storeCode || "").toString().trim();
    const locationTitle = (loc.title || "").trim();

    if (!locationId) return;

    let reviews;
    try {
      reviews = await listReviewsForLocation(
        accessToken, ENV.GBP_ACCOUNT_ID, locationId, start, end
      );
    } catch (e) {
      console.warn(`  ⚠ ${storeCode || locationTitle}: ${e.message}`);
      return;
    }

    if (!reviews.length) return;
    console.log(`  ${storeCode || locationTitle}: ${reviews.length} review(s)`);

    for (const r of reviews) {
      const dt       = DateTime.fromISO(r.createTime, { setZone: true }).setZone(TZ);
      const rating   = starRatingToInt(r.starRating);
      const reviewer = (r.reviewer?.displayName || r.reviewer?.profileName || "").trim();
      const comment  = cleanComment(r.comment || "");

      const reply     = cleanComment(r.reviewReply?.comment || "");
      const repliedAt = r.reviewReply?.updateTime
        ? DateTime.fromISO(r.reviewReply.updateTime, { setZone: true })
            .setZone(TZ)
            .toFormat("dd.MM.yyyy HH:mm")
        : null;

      rows.push({
        Date:      dt.toFormat("dd.MM.yyyy"),
        Time:      dt.toFormat("HH:mm:ss"),
        Rating:    rating,
        Store:     storeCode || null,
        Comment:   comment || null,
        Reviewer:  reviewer || null,
        Reply:     reply || null,
        RepliedAt: repliedAt || null,
        Channel:   "Google",
      });

      await sleep(60);
    }
  });

  rows.sort((a, b) => {
    const dtA = DateTime.fromFormat(`${a.Date} ${a.Time}`, "dd.MM.yyyy HH:mm:ss", { zone: TZ });
    const dtB = DateTime.fromFormat(`${b.Date} ${b.Time}`, "dd.MM.yyyy HH:mm:ss", { zone: TZ });
    return dtB.toMillis() - dtA.toMillis();
  });

  console.log(`\n✓ Total reviews: ${rows.length}`);

  // -------------------- CSV Export --------------------
  const dateFrom = start.toFormat("yyyy-MM-dd");
  const dateTo   = end.toFormat("yyyy-MM-dd");
  const filename = `gbp-reviews-${dateFrom}..${dateTo}.csv`;

  const csv = Papa.unparse(rows);
  fs.writeFileSync(filename, "\uFEFF" + csv, "utf-8");
  console.log(`\n📄 CSV gespeichert: ${filename}`);

  // -------------------- Make Webhook (optional) --------------------
  if (ENV.MAKE_REVIEWS_WEBHOOK_URL_MONTHLY) {
    const res = await requestWithRetry(ENV.MAKE_REVIEWS_WEBHOOK_URL_MONTHLY, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type:      "gbp_reviews_history",
        dateFrom,
        dateTo,
        row_count: rows.length,
        rows,
      }),
    });
    const txt = await res.text().catch(() => "");
    console.log(`🚀 Make Webhook → ${res.status} ${txt.slice(0, 100)}`);
  } else {
    console.log("ℹ️  MAKE_REVIEWS_WEBHOOK_URL_MONTHLY nicht gesetzt – Webhook übersprungen");
  }

  console.log("\n✅ Fertig");
}

main().catch((e) => {
  console.error("\n❌ ERROR:", e?.message || e);
  process.exit(1);
});
