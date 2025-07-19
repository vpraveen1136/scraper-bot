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

async function updateSheetFromCSV(csvData, csvDateStr) {
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
  console.log(`📄 Loading cells A1:AF${rowCount}...`);
  await sheet.loadCells(`A1:AF${rowCount}`);

  // Parse Q2 date
  const q2Cell = sheet.getCell(1, 16); // Q2
  const existingDateStr = q2Cell.value ? String(q2Cell.value).trim() : "";
  const [exDay, exMonth, exYear] = existingDateStr.split("/").map(Number);
  const existingDate = new Date(exYear, exMonth - 1, exDay);

  // Parse new CSV date (dd-MMM-yyyy)
  const [newDay, newMonStr, newYear] = csvDateStr.split("-");
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11,
  };
  const newDate = new Date(parseInt(newYear), monthMap[newMonStr], parseInt(newDay));

  console.log("📅 Existing Q2 Date:", existingDateStr, "→", existingDate.toDateString());
  console.log("📅 New CSV Date:", csvDateStr, "→", newDate.toDateString());

  if (!isNaN(existingDate) && newDate <= existingDate) {
    console.log("⚠️ Sheet already contains newer or same date:", existingDateStr);
    return;
  }

  // Shift C–P → B–O and S–AE → R–AD
  for (let r = 1; r < rowCount; r++) {
    for (let c = 2; c <= 15; c++) {
      const from = sheet.getCell(r, c);
      const to = sheet.getCell(r, c - 1);
      to.value = from.value;
    }
    for (let c = 18; c <= 31; c++) {
      const from = sheet.getCell(r, c);
      const to = sheet.getCell(r, c - 1);
      to.value = from.value;
    }
  }

  // Prepare maps
  const deliveryMap = {};
  const priceChgMap = {};
  for (const row of csvData) {
    const symbol = row["SYMBOL"]?.trim().toUpperCase();
    const delivery = row["DELIV_PER"]?.trim();
    const today = parseFloat(row["CLOSE_PRICE"]);
    const prev = parseFloat(row["PREV_CLOSE"]);

    if (symbol && delivery) {
      deliveryMap[symbol] = delivery;
    }

    if (symbol && !isNaN(today) && !isNaN(prev) && prev !== 0) {
      priceChgMap[symbol] = ((today - prev) / prev) * 100;
    }
  }

  // Update column P (15) - Delivery %
  let deliveryCount = 0;
  for (let r = 1; r < rowCount; r++) {
    const symbol = sheet.getCell(r, 0)?.value?.toString().trim().toUpperCase();
    const cell = sheet.getCell(r, 15);
    const newVal = deliveryMap[symbol];
    if (symbol && newVal && cell.value !== newVal) {
      cell.value = newVal;
      deliveryCount++;
    }
  }

  // Write Q2 new date
  q2Cell.value = `${newDay.padStart(2, "0")}/${(monthMap[newMonStr] + 1).toString().padStart(2, "0")}/${newYear}`;

  console.log("💾 Saving delivery % and column shifts...");
  await sheet.saveUpdatedCells();
  console.log(`✅ Saved ${deliveryCount} delivery updates and date to Q2.`);

  // Now update column AE (31) - Price %
  let priceCount = 0;
  for (let r = 1; r < rowCount; r++) {
    const symbol = sheet.getCell(r, 0)?.value?.toString().trim().toUpperCase();
    const cell = sheet.getCell(r, 31);
    const priceChange = priceChgMap[symbol];
    if (symbol && priceChange != null && cell.value !== priceChange) {
      cell.value = parseFloat(priceChange.toFixed(2));
      priceCount++;
    }
  }

  console.log("💾 Saving price % updates...");
  await sheet.saveUpdatedCells();
  console.log(`✅ Sheet updated: ${deliveryCount} delivery rows, ${priceCount} price rows.`);
}


async function main() {
  try {
    console.log("🟡 Starting script...");
    const urlDateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${urlDateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_18072025.csv";
    console.log("📥 Downloading CSV:", url);

    const csvData = await downloadCSV(url);
    console.log("📊 Records downloaded:", csvData.length);

    const firstRow = csvData[0];
    const csvDateStr = firstRow && firstRow["DATE1"];
    if (!csvDateStr) throw new Error("DATE1 field (CSV C2) not found");

    await updateSheetFromCSV(csvData, csvDateStr);
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
