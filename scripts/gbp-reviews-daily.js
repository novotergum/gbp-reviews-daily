import "dotenv/config";
import { DateTime } from "luxon";

// -------------------- ENV --------------------
const ENV = {
  GBP_CLIENT_ID: (process.env.GBP_CLIENT_ID || "").trim(),
  GBP_CLIENT_SECRET: (process.env.GBP_CLIENT_SECRET || "").trim(),
  GBP_REFRESH_TOKEN: (process.env.GBP_REFRESH_TOKEN || "").trim(),
  GBP_ACCOUNT_ID: (process.env.GBP_ACCOUNT_ID || "").trim(),

  PREFILL_API_URL: (process.env.PREFILL_API_URL || "").trim(),
  PREFILL_SECRET: (process.env.PREFILL_SECRET || "").trim(),
  PUBLIC_APP_URL: (process.env.PUBLIC_APP_URL || "https://smart-reply-generator-production2.up.railway.app").trim(),

  MAKE_REVIEWS_WEBHOOK_URL: (process.env.MAKE_REVIEWS_WEBHOOK_URL || "").trim(),

  // optional tuning
  CONCURRENCY: Number(process.env.CONCURRENCY || "5"),
  MAKE_BATCH_SIZE: Number(process.env.MAKE_BATCH_SIZE || "200"),
};

function mustEnv(key) {
  if (!ENV[key]) throw new Error(`Missing env: ${key}`);
  return ENV[key];
}

function mask(s) {
  if (!s) return "";
  if (s.length <= 8) return "***";
  return `${s.slice(0, 3)}***${s.slice(-3)}`;
}

// -------------------- TIME RANGE (Yesterday in Europe/Berlin) --------------------
const TZ = "Europe/Berlin";

function getYesterdayRangeBerlin() {
  const now = DateTime.now().setZone(TZ);
  const y = now.minus({ days: 1 });
  const start = y.startOf("day");
  const end = y.endOf("day");
  return { start, end };
}

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
        const txt = await res.text().catch(() => "");
        lastErr = new Error(`HTTP ${res.status} ${res.statusText}: ${txt}`);
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
      if (v !== undefined && v !== null && String(v).length > 0) u.searchParams.set(k, String(v));
    }
  }

  const res = await requestWithRetry(u.toString(), { headers, method: "GET" });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`GET ${u.toString()} -> ${res.status}: ${txt}`);
  }
  return await res.json();
}

// -------------------- Google OAuth (Refresh Token -> Access Token) --------------------
async function getAccessToken() {
  mustEnv("GBP_CLIENT_ID");
  mustEnv("GBP_CLIENT_SECRET");
  mustEnv("GBP_REFRESH_TOKEN");

  const body = new URLSearchParams({
    client_id: ENV.GBP_CLIENT_ID,
    client_secret: ENV.GBP_CLIENT_SECRET,
    refresh_token: ENV.GBP_REFRESH_TOKEN,
    grant_type: "refresh_token",
  });

  const res = await requestWithRetry("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  const txt = await res.text();
  if (!res.ok) throw new Error(`Token error ${res.status}: ${txt}`);

  const j = JSON.parse(txt);
  if (!j.access_token) throw new Error(`No access_token in token response: ${txt}`);
  return j.access_token;
}

// -------------------- GBP APIs --------------------

// Locations: Business Information API v1
async function listLocations(accessToken, accountId) {
  const out = [];
  let pageToken = "";

  do {
    const j = await getJson(
      `https://mybusinessbusinessinformation.googleapis.com/v1/accounts/${accountId}/locations`,
      {
        headers: { Authorization: `Bearer ${accessToken}` },
        params: {
          pageSize: "100",
          // NEW: metadata.* mitziehen (mapsUri, newReviewUri, placeId)
          readMask: "name,title,storeCode,metadata.mapsUri,metadata.newReviewUri,metadata.placeId",
          orderBy: "storeCode",
          pageToken,
        },
      }
    );

    const locs = j.locations || [];
    out.push(...locs);

    pageToken = j.nextPageToken || "";
  } while (pageToken);

  return out;
}

// NEW: Fallback, falls metadata im List-Call nicht geliefert wird (oder leer ist)
async function getLocationMetadata(accessToken, locationId) {
  const j = await getJson(`https://mybusinessbusinessinformation.googleapis.com/v1/locations/${locationId}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
    params: {
      readMask: "metadata.mapsUri,metadata.newReviewUri,metadata.placeId",
    },
  });
  return j?.metadata || {};
}

// Reviews: My Business API v4
async function listReviewsForLocation(accessToken, accountId, locationId, startBerlin, endBerlin) {
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
          orderBy: "updateTime desc",
          pageToken,
        },
      }
    );

    const reviews = j.reviews || [];
    if (reviews.length === 0) break;

    let anyGeStart = false;

    for (const r of reviews) {
      const ct = r.createTime;
      if (!ct) continue;

      const dtBerlin = DateTime.fromISO(ct, { setZone: true }).setZone(TZ);

      if (dtBerlin.toMillis() >= startBerlin.toMillis()) anyGeStart = true;

      if (dtBerlin.toMillis() >= startBerlin.toMillis() && dtBerlin.toMillis() <= endBerlin.toMillis()) {
        out.push(r);
      }
    }

    if (!anyGeStart) pagesBelowCutoff += 1;
    else pagesBelowCutoff = 0;

    if (pagesBelowCutoff >= 2) break;

    pageToken = j.nextPageToken || "";
    if (pageToken) await sleep(120);
  } while (pageToken);

  return out;
}

function starRatingToInt(star) {
  if (!star) return null;
  const s = String(star).toUpperCase();
  const map = { ONE: 1, TWO: 2, THREE: 3, FOUR: 4, FIVE: 5 };
  return map[s] ?? null;
}

// -------------------- Comment cleaning --------------------
function cleanComment(text) {
  let t = (text || "").trim();
  const markers = ["(Translated by Google)", "(Übersetzt von Google)"];
  for (const m of markers) {
    const idx = t.indexOf(m);
    if (idx !== -1) t = t.slice(0, idx).trimEnd();
  }
  return t;
}

function buildCommentFull(comment, reviewer, reviewedAt) {
  const base = (comment || "").trim() || "(kein Kommentar)";
  const name = (reviewer || "").trim() || "Unbekannt";
  let suffix = `— ${name}`;
  if (reviewedAt) suffix += `, am ${reviewedAt}`;
  return `${base}\n${suffix}`;
}

// -------------------- Prefill API --------------------
async function createPrefillRid(payload) {
  mustEnv("PREFILL_API_URL");
  mustEnv("PREFILL_SECRET");

  const res = await requestWithRetry(ENV.PREFILL_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Prefill-Secret": ENV.PREFILL_SECRET,
    },
    body: JSON.stringify(payload),
  });

  const txt = await res.text();
  if (!res.ok) throw new Error(`Prefill error ${res.status}: ${txt}`);

  const j = JSON.parse(txt);
  if (!j.rid) throw new Error(`Prefill response missing rid: ${txt}`);
  return j.rid;
}

// -------------------- Make Webhook --------------------
async function postToMake(payload) {
  mustEnv("MAKE_REVIEWS_WEBHOOK_URL");

  const res = await requestWithRetry(ENV.MAKE_REVIEWS_WEBHOOK_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const txt = await res.text().catch(() => "");
  console.log(`Make response: ${res.status}`);
  if (txt) console.log(`Make body (first 500 chars): ${txt.slice(0, 500)}`);

  if (!res.ok) throw new Error(`Make webhook error ${res.status}: ${txt}`);
}

function chunkArray(arr, size) {
  if (size <= 0) return [arr];
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// -------------------- Concurrency pool --------------------
async function asyncPool(limit, items, iteratorFn) {
  const ret = [];
  const executing = [];

  for (const item of items) {
    const p = Promise.resolve().then(() => iteratorFn(item));
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
  mustEnv("GBP_ACCOUNT_ID");
  mustEnv("MAKE_REVIEWS_WEBHOOK_URL");
  mustEnv("PREFILL_API_URL");
  mustEnv("PREFILL_SECRET");

  const { start, end } = getYesterdayRangeBerlin();

  console.log(`TZ: ${TZ}`);
  console.log(`Range (yesterday Berlin): ${start.toISO()} -> ${end.toISO()}`);
  console.log(`Account: ${ENV.GBP_ACCOUNT_ID}`);
  console.log(`Prefill API: ${ENV.PREFILL_API_URL}`);
  console.log(`Make webhook: ${mask(ENV.MAKE_REVIEWS_WEBHOOK_URL)}`);

  console.log("\n1) Access token …");
  const accessToken = await getAccessToken();
  console.log("✓ token ok");

  console.log("\n2) Locations …");
  const locations = await listLocations(accessToken, ENV.GBP_ACCOUNT_ID);
  console.log(`✓ locations: ${locations.length}`);

  const items = [];

  console.log("\n3) Reviews (all locations, yesterday) …");

  await asyncPool(ENV.CONCURRENCY, locations, async (loc) => {
    const locName = (loc.name || "").trim(); // e.g. "locations/123"
    const locationId = locName.split("/").pop();
    const storeCode = (loc.storeCode || "").toString().trim();
    const locationTitle = (loc.title || "").trim();

    if (!locationId) return;

    // NEW: Location-Metadaten (Maps-Link + Review-Link + PlaceId)
    let maps_uri = loc?.metadata?.mapsUri || "";
    let new_review_uri = loc?.metadata?.newReviewUri || "";
    let place_id = loc?.metadata?.placeId || "";

    // Fallback, falls im listLocations nicht befüllt
    if (!maps_uri && !new_review_uri && !place_id) {
      try {
        const meta = await getLocationMetadata(accessToken, locationId);
        maps_uri = meta?.mapsUri || "";
        new_review_uri = meta?.newReviewUri || "";
        place_id = meta?.placeId || "";
      } catch {
        // fail-soft: Metadata ist nice-to-have, Reviews sind core
      }
    }

    // Optional: stabiler Maps-Link aus placeId (falls du lieber standardisieren willst)
    const maps_place_url = place_id
      ? `https://www.google.com/maps/place/?q=place_id:${encodeURIComponent(place_id)}`
      : null;

    let reviews;
    try {
      reviews = await listReviewsForLocation(accessToken, ENV.GBP_ACCOUNT_ID, locationId, start, end);
    } catch (e) {
      console.log(`- ERROR reviews ${storeCode || locationTitle || locationId}: ${e.message}`);
      return;
    }

    if (!reviews.length) return;

    console.log(`- ${storeCode || locationTitle || locationId}: ${reviews.length} review(s)`);

    for (const r of reviews) {
      const reviewName = (r.name || "").trim();
      const reviewId = reviewName.split("/").pop() || "";

      const rating = starRatingToInt(r.starRating);
      const reviewerObj = r.reviewer || {};
      const reviewer = (reviewerObj.displayName || reviewerObj.profileName || "").trim();

      const createdBerlin = DateTime.fromISO(r.createTime, { setZone: true }).setZone(TZ);
      const reviewed_at = createdBerlin.toFormat("dd.MM.yyyy HH:mm:ss");

      const commentClean = cleanComment(r.comment || "");
      const comment_full = buildCommentFull(commentClean, reviewer, reviewed_at);

      let rid = "";
      let smart_reply_url = "";
      let prefill_error = "";

      try {
        rid = await createPrefillRid({
          review: comment_full,
          rating: rating ? String(rating) : "",

          reviewer,
          reviewed_at,
          accountId: ENV.GBP_ACCOUNT_ID,
          locationId,
          reviewId,
          storeCode,
          locationTitle,

          // NEW: Links/IDs für Location
          maps_uri: maps_uri || "",
          new_review_uri: new_review_uri || "",
          place_id: place_id || "",
          maps_place_url: maps_place_url || "",
        });
        smart_reply_url = `${ENV.PUBLIC_APP_URL.replace(/\/$/, "")}/?rid=${rid}`;
      } catch (e) {
        prefill_error = e.message || String(e);
      }

      items.push({
        storeCode: storeCode || null,
        locationTitle: locationTitle || null,
        locationId,
        reviewId: reviewId || null,
        rating: rating ?? null,
        reviewer: reviewer || null,
        reviewed_at,
        comment: commentClean || null,
        comment_full,
        prefill_rid: rid || null,
        smart_reply_url: smart_reply_url || null,
        prefill_error: prefill_error || null,

        // NEW: Links/IDs für Location
        maps_uri: maps_uri || null,
        new_review_uri: new_review_uri || null,
        place_id: place_id || null,
        maps_place_url: maps_place_url || null,
      });

      await sleep(60);
    }
  });

  console.log(`\n✓ total reviews (yesterday): ${items.length}`);

  // 4) Send to Make (chunked)
  console.log("\n4) Send to Make …");

  const chunks = chunkArray(items, ENV.MAKE_BATCH_SIZE);
  const metaBase = {
    source: "google_business_profile",
    timezone: TZ,
    range_start: start.toISO(),
    range_end: end.toISO(),
    generated_at: DateTime.now().setZone(TZ).toISO(),
    account_id: ENV.GBP_ACCOUNT_ID,
    locations_total: locations.length,
    count_total: items.length,
  };

  if (chunks.length === 0) chunks.push([]);

  for (let i = 0; i < chunks.length; i++) {
    const payload = {
      ...metaBase,
      batch_index: i + 1,
      batch_total: chunks.length,
      count: chunks[i].length,
      data: chunks[i],
    };
    await postToMake(payload);
  }

  console.log("✓ done");
}

main().catch((e) => {
  console.error("\nERROR:", e?.message || e);
  process.exit(1);
});
