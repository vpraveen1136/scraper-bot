const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
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

async function updateSheetFromCSV(csvData, urlDateStr) {
  const doc = new GoogleSpreadsheet(SHEET_ID);
  console.log("🟡 Authenticating Google Sheets...");
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo();

  const sheet = doc.sheetsByTitle["DELprc"];
  if (!sheet) {
    console.error("❌ Sheet 'DELprc' not found");
    return;
  }

  const deliveryMap = {};
  for (const row of csvData) {
    const symbol = row["SYMBOL"]?.trim().toUpperCase();
    const delivery = row["DELIV_PER"]?.trim();
    if (symbol && delivery) {
      deliveryMap[symbol] = delivery;
    }
  }

  const rowCount = sheet.rowCount;
  console.log(`📄 Loading cells A1:Q${rowCount}...`);
  await sheet.loadCells(`A1:Q${rowCount}`);

  // 🔎 Read Q2 and compare dates
  const q2Cell = sheet.getCell(1, 16); // Q2
  const existingDateStr = q2Cell.value ? String(q2Cell.value).trim() : "";
  const [exDay, exMonth, exYear] = existingDateStr.split("/").map(Number);
  const existingDate = new Date(exYear, exMonth - 1, exDay);

  const urlDay = parseInt(urlDateStr.slice(0, 2));
  const urlMonth = parseInt(urlDateStr.slice(2, 4));
  const urlYear = parseInt(urlDateStr.slice(4));
  const urlDateObj = new Date(urlYear, urlMonth - 1, urlDay);

  if (!isNaN(existingDate.getTime()) && urlDateObj <= existingDate) {
    console.log("⚠️ Sheet already contains newer or same date:", existingDateStr);
    return;
  }

  // ⏩ Copy columns C to P → B to O
  for (let r = 1; r < rowCount; r++) {
    for (let c = 2; c <= 15; c++) {
      const sourceCell = sheet.getCell(r, c);
      const targetCell = sheet.getCell(r, c - 1);
      targetCell.value = sourceCell.value;
    }
  }

  // ✅ Update column P with new delivery data
  let updatedCount = 0;
  for (let r = 1; r < rowCount; r++) {
    const symbolCell = sheet.getCell(r, 0); // Column A
    const deliveryCell = sheet.getCell(r, 15); // Column P
    const symbol = symbolCell.value?.toString().trim().toUpperCase();
    const delivery = deliveryMap[symbol];

    if (symbol && delivery && deliveryCell.value !== delivery) {
      deliveryCell.value = delivery;
      updatedCount++;
      console.log(`✅ Updated ${symbol} → ${delivery}`);
    }
  }

  // 📝 Write new date to Q2
  const formattedDate = `${urlDay.toString().padStart(2, "0")}/${urlMonth
    .toString()
    .padStart(2, "0")}/${urlYear}`;
  q2Cell.value = formattedDate;

  await sheet.saveUpdatedCells();
  console.log(`✅ Batch update complete. ${updatedCount} rows updated.`);
}

async function main() {
  try {
    console.log("🟡 Starting script...");
    const urlDateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${urlDateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_13072025.csv";

    console.log("📥 Downloading CSV:", url);
    const csvData = await downloadCSV(url);
    console.log("📊 Records downloaded:", csvData.length);

    await updateSheetFromCSV(csvData, urlDateStr);
    console.log("✅ Sheet updated successfully.");
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
