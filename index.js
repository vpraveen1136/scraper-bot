
const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
//require('dotenv').config();
const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const SHEET_ID = "1EVY2xoDUVJ5BtTiStQKGqWURCD5AH4sSZcw5uHaYrUI";
const axios = require("axios");

function getTodayDateString() {
  const now = new Date();
  const dd = String(now.getDate()).padStart(2, "0");
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const yyyy = now.getFullYear();
  return `${dd}${mm}${yyyy}`;
}

function downloadCSV(url) {
  return new Promise(async (resolve, reject) => {
    const results = [];

    try {
      const response = await axios.get(url, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Accept": "*/*",
          "Referer": "https://www.nseindia.com/",
          "Host": "nsearchives.nseindia.com",
        },
        timeout: 20000,
        responseType: "stream",
      });

      response.data
        .pipe(csv())
        .on("data", (rawData) => {
          const cleanData = {};
          for (const key in rawData) {
            const cleanKey = key.replace(/\uFEFF/g, "").trim();
            cleanData[cleanKey] = rawData[key]?.trim();
          }
          results.push(cleanData);
        })
        .on("end", () => resolve(results))
        .on("error", (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}

async function updateSheetFromCSV(csvData) {
  const doc = new GoogleSpreadsheet(SHEET_ID);
  console.log("🟡 Authenticating Google Sheets...");
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo();

  const sheet = doc.sheetsByTitle["DelPerc"];
  if (!sheet) {
    console.error("❌ Sheet 'DelPerc' not found");
    return;
  }

  const rows = await sheet.getRows();
  const deliveryMap = {};

  for (const row of csvData) {
    const symbol = row["SYMBOL"]?.trim().toUpperCase();
    const delivery = row["DELIV_PER"]?.trim();
    if (symbol && delivery) {
      deliveryMap[symbol] = delivery;
    }
  }

  console.log(`📄 Updating 'DelPerc' → Rows: ${rows.length}`);
  for (const row of rows) {
    const rawSymbol = row._rawData[0];
    const symbol = rawSymbol?.trim().toUpperCase();
    const delivery = deliveryMap[symbol];

    if (symbol && delivery) {
      row._rawData[1] = delivery; // Column B = index 1
      await row.save();
      console.log(`✅ Updated ${symbol} → ${delivery}`);
    } else {
      console.log(`⚠️  Symbol '${symbol}' not found in Bhavcopy`);
    }
  }
}

async function main() {
  try {
    console.log("🟡 Starting script...");
    const dateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${dateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_15072025.csv";
    console.log("📥 Downloading CSV:", url);

    const csvData = await downloadCSV(url);
    console.log("📊 Records downloaded:", csvData.length);

    await updateSheetFromCSV(csvData);
    console.log("✅ Sheet updated successfully.");
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
