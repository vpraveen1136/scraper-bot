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

  const rowCount = sheet.rowCount;
  console.log(`📄 Loading cells A1:Q${rowCount}...`);
  await sheet.loadCells(`A1:Q${rowCount}`);

  const q2Cell = sheet.getCell(1, 16); // Q2
  const existingDate = q2Cell.value?.trim();

  // Compare dates
  if (existingDate && urlDate <= existingDate) {
    console.log("⚠️ Sheet already contains newer or same date:", existingDate);
    return;
  }

  // Backup: Copy C:P to B:O
  console.log("📋 Backing up C:P → B:O...");
  for (let r = 1; r < rowCount; r++) {
    for (let c = 2; c <= 15; c++) {
      const fromCell = sheet.getCell(r, c);
      const toCell = sheet.getCell(r, c - 1);
      toCell.value = fromCell.value;
    }
  }

  // Prepare delivery % update map
  const deliveryMap = {};
  for (const row of csvData) {
    const symbol = row["SYMBOL"]?.trim().toUpperCase();
    const delivery = row["DELIV_PER"]?.trim();
    if (symbol && delivery) {
      deliveryMap[symbol] = delivery;
    }
  }

  // Update delivery % in column P (15)
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

  // Write URL date to Q2
  q2Cell.value = urlDate;

  await sheet.saveUpdatedCells();
  console.log(`✅ Update complete. ${updatedCount} rows modified. Date recorded in Q2.`);
}

async function main() {
  try {
    console.log("🟡 Starting script...");
    const dateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${dateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_05072025.csv";

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
