const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const SHEET_ID = "1EVY2xoDUVJ5BtTiStQKGqWURCD5AH4sSZcw5uHaYrUI";
const axios = require("axios");

function parseCSVDate(csvDateStr) {
  const months = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11
  };
  const [day, monthStr, year] = csvDateStr.split("-");
  return new Date(parseInt(year), months[monthStr], parseInt(day));
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

      let isFirst = true;
      let csvDate = "";

      response.data
        .pipe(csv())
        .on("data", (rawData) => {
          if (isFirst) {
            csvDate = rawData["DATE1"] || rawData["DATE"]; // fallback
            isFirst = false;
          }
          const cleanData = {};
          for (const key in rawData) {
            const cleanKey = key.replace(/\uFEFF/g, "").trim();
            cleanData[cleanKey] = rawData[key]?.trim();
          }
          results.push(cleanData);
        })
        .on("end", () => resolve({ data: results, date: csvDate }))
        .on("error", (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}

async function updateSheet(csvData, csvDateStr) {
  const doc = new GoogleSpreadsheet(SHEET_ID);
  console.log("🟡 Authenticating Google Sheets...");
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo();

  const sheet = doc.sheetsByTitle["DELprc"];
  if (!sheet) {
    console.error("❌ Sheet 'DELprc' not found");
    return;
  }

  const rowCount = sheet.rowCount;
  console.log(`📄 Loading cells A1:Q${rowCount}...`);
  await sheet.loadCells(`A1:Q${rowCount}`);

  // 📆 Read Q2 date from sheet
  const q2Cell = sheet.getCell(1, 16); // Q2
  const sheetDateStr = q2Cell.value ? String(q2Cell.value).trim() : "";
  const [sd, sm, sy] = sheetDateStr.split("/").map(Number);
  const sheetDate = new Date(sy, sm - 1, sd);

  // 📆 Parse date from CSV C2
  const csvDate = parseCSVDate(csvDateStr);

  if (!isNaN(sheetDate.getTime()) && csvDate <= sheetDate) {
    console.log("⚠️ Sheet already contains newer or same date:", sheetDateStr);
    return;
  }

  // 🗂️ Create delivery map from SYMBOL → DELIV_PER
  const deliveryMap = {};
  for (const row of csvData) {
    const symbol = row["SYMBOL"]?.trim().toUpperCase();
    const delivery = row["DELIV_PER"]?.trim();
    if (symbol && delivery) {
      deliveryMap[symbol] = delivery;
    }
  }

  // 🔁 Shift columns C→P → B→O
  for (let r = 1; r < rowCount; r++) {
    for (let c = 2; c <= 15; c++) {
      const sourceCell = sheet.getCell(r, c);
      const targetCell = sheet.getCell(r, c - 1);
      targetCell.value = sourceCell.value;
    }
  }

  // 🔄 Update column P
  let updatedCount = 0;
  for (let r = 1; r < rowCount; r++) {
    const symbolCell = sheet.getCell(r, 0);
    const deliveryCell = sheet.getCell(r, 15); // P

    const symbol = symbolCell.value?.toString().trim().toUpperCase();
    const delivery = deliveryMap[symbol];

    if (symbol && delivery && deliveryCell.value !== delivery) {
      deliveryCell.value = delivery;
      updatedCount++;
      console.log(`✅ Updated ${symbol} → ${delivery}`);
    }
  }

  // 📝 Write CSV C2 date to Q2
  const dd = csvDate.getDate().toString().padStart(2, "0");
  const mm = (csvDate.getMonth() + 1).toString().padStart(2, "0");
  const yyyy = csvDate.getFullYear();
  q2Cell.value = `${dd}/${mm}/${yyyy}`;

  await sheet.saveUpdatedCells();
  console.log(`✅ Sheet updated. ${updatedCount} rows changed. Date: ${q2Cell.value}`);
}

async function main() {
  try {
    console.log("🟡 Starting script...");
    const urlDateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${urlDateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_12072025.csv";

    console.log("📥 Downloading CSV:", url);
    const { data, date } = await downloadCSV(url);

    console.log("📊 Records downloaded:", data.length);
    console.log("📅 CSV C2 date:", date);

    await updateSheet(data, date);
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
