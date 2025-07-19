const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
const axios = require("axios");

const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const SHEET_ID = "1EVY2xoDUVJ5BtTiStQKGqWURCD5AH4sSZcw5uHaYrUI";

function getTodayDateString() {
  const now = new Date();
  const dd = String(now.getDate()).padStart(2, "0");
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const yyyy = now.getFullYear();
  return `${dd}${mm}${yyyy}`;
}

function parseDDMMYYYYtoDate(ddmmyyyy) {
  const dd = parseInt(ddmmyyyy.slice(0, 2));
  const mm = parseInt(ddmmyyyy.slice(2, 4)) - 1; // 0-based months
  const yyyy = parseInt(ddmmyyyy.slice(4, 8));
  return new Date(yyyy, mm, dd);
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

async function updateSheetFromCSV(csvData, urlDate) {
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

  const q2Cell = sheet.getCell(1, 16); // Q2
  const existingDate = q2Cell.value instanceof Date
    ? q2Cell.value
    : new Date(String(q2Cell.value));

  const urlDateObj = new Date(
    `${urlDate.slice(4)}-${urlDate.slice(2, 4)}-${urlDate.slice(0, 2)}`
  );

  if (existingDate && urlDateObj <= existingDate) {
    console.log("⚠️ Sheet already contains newer or same date:", existingDate);
    return;
  }

  let updatedCount = 0;

  for (let r = 1; r < rowCount; r++) {
    const symbolCell = sheet.getCell(r, 0);  // Column A (symbol)
    const deliveryCell = sheet.getCell(r, 15); // Column P (delivery %)

    const symbol = symbolCell.value?.toString().trim().toUpperCase();
    const delivery = deliveryMap[symbol];

    if (symbol && delivery && deliveryCell.value !== delivery) {
      deliveryCell.value = delivery;
      updatedCount++;
      console.log(`✅ Updated ${symbol} → ${delivery}`);
    }
  }

  // Copy values C to P into B to O if date is newer
  for (let r = 1; r < rowCount; r++) {
    for (let c = 2; c <= 15; c++) {
      const fromCell = sheet.getCell(r, c);
      const toCell = sheet.getCell(r, c - 1);
      toCell.value = fromCell.value;
    }
  }

  // Set date in Q2
  q2Cell.value = urlDateObj;

  try {
    console.log("💾 Saving updated cells to Google Sheet...");
    await sheet.saveUpdatedCells();
    console.log(`✅ Update complete. ${updatedCount} rows modified. Date written to Q2.`);
  } catch (err) {
    console.error("❌ Failed to save updates to sheet:", err.message);
  }
}


async function main() {
  try {
    console.log("🟡 Starting script...");
    const dateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${dateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_07072025.csv";

    console.log("📥 Downloading CSV:", url);
    const csvData = await downloadCSV(url);
    console.log("📊 Records downloaded:", csvData.length);

    await updateSheetFromCSV(csvData, dateStr);
    console.log("✅ Sheet updated successfully.");
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
