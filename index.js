const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const SHEET_ID = "1EVY2xoDUVJ5BtTiStQKGqWURCD5AH4sSZcw5uHaYrUI";
const axios = require("axios");

// Create today's date string in ddmmyyyy format for URL
function getTodayDateString() {
  const now = new Date();
  const dd = String(now.getDate()).padStart(2, "0");
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const yyyy = now.getFullYear();
  return `${dd}${mm}${yyyy}`;
}

// Download CSV and parse
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
        .on("data", (row) => {
          const cleanRow = {};
          for (let key in row) {
            const cleanKey = key.replace(/\uFEFF/g, "").trim();
            cleanRow[cleanKey] = row[key]?.trim();
          }
          results.push(cleanRow);
        })
        .on("end", () => resolve(results))
        .on("error", (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}

// Sleep function for delay between column shifts
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Shift one column at a time with delay
async function shiftColumnsWithDelay(sheet, startRow, endRow, sourceStart, sourceEnd, offset = -1) {
  for (let c = sourceStart; c <= sourceEnd; c++) {
    const fromColLetter = String.fromCharCode(65 + c);
    const toColLetter = String.fromCharCode(65 + c + offset);
    console.log(`📥 Copying ${fromColLetter}${startRow + 1}:${fromColLetter}${endRow + 1} → ${toColLetter}${startRow + 1}:${toColLetter}${endRow + 1}`);

    for (let r = startRow; r <= endRow; r++) {
      const from = sheet.getCell(r, c);
      const to = sheet.getCell(r, c + offset);
      to.value = from.value;
    }

    await sheet.saveUpdatedCells();
    console.log(`✅ Saved column ${fromColLetter} → ${toColLetter}`);
    await sleep(1000); // wait 1 second
  }
}

// Main update function
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
  await sheet.loadCells(`A1:AF${rowCount}`);

  // Parse Q2 date
  const q2Cell = sheet.getCell(1, 16); // Q2
  const existingDateStr = q2Cell.value ? String(q2Cell.value).trim() : "";
  const [exDay, exMonth, exYear] = existingDateStr.split("/").map(Number);
  const existingDate = new Date(exYear, exMonth - 1, exDay);

  // Parse CSV date from DATE1
  const [newDay, newMonStr, newYear] = csvDateStr.split("-");
  const monthMap = {
    Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
    Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11,
  };
  const newDate = new Date(parseInt(newYear), monthMap[newMonStr], parseInt(newDay));

  console.log("📅 Existing Q2 Date:", existingDateStr, "→", existingDate.toDateString());
  console.log("📅 New CSV Date:", csvDateStr, "→", newDate.toDateString());

  if (!isNaN(existingDate) && newDate <= existingDate) {
    console.log("⚠️ Sheet already contains newer or same date. No update needed.");
    return;
  }

  // ✅ Shift columns with delay
  await shiftColumnsWithDelay(sheet, 1, rowCount - 1, 2, 15, -1);  // C:P → B:O
  await shiftColumnsWithDelay(sheet, 1, rowCount - 1, 18, 31, -1); // S:AE → R:AD

// Prepare data maps
const deliveryValueMap = {};
const priceMap = {};

for (const row of csvData) {
  const symbol = row["SYMBOL"]?.trim().toUpperCase();
  const delivQty = parseFloat(row["DELIV_QTY"]);
  const avgPrice = parseFloat(row["AVG_PRICE"]);
  const today = parseFloat(row["CLOSE_PRICE"]);
  const prev = parseFloat(row["PREV_CLOSE"]);

  if (symbol && !isNaN(delivQty) && !isNaN(avgPrice)) {
    deliveryValueMap[symbol] = delivQty * avgPrice/10000000;
  }

  if (symbol && !isNaN(today) && !isNaN(prev) && prev !== 0) {
    priceMap[symbol] = ((today - prev) / prev) * 100; // % change
  }
}


  // Update delivery value (P column)
  let deliveryCount = 0;
  for (let r = 1; r < rowCount; r++) {
    const symbol = sheet.getCell(r, 0).value?.toString().trim().toUpperCase();
    if (!symbol) continue;
    const newDeliv = deliveryValueMap[symbol];
    const cell = sheet.getCell(r, 15);
    if (newDeliv) {
      const parsedDeliv = parseFloat(newDeliv);
      if (!isNaN(parsedDeliv)) {
        const roundedDeliv = Math.round(parsedDeliv);  // 👈 convert to whole number
        if (cell.value !== roundedDeliv) {
          cell.value = roundedDeliv;
          deliveryCount++;
        }
      }
    }
  }

  await sheet.saveUpdatedCells();
  console.log(`✅ Updated ${deliveryCount} delivery rows.`);

  // Update price % (AE column)
  let priceCount = 0;
  for (let r = 1; r < rowCount; r++) {
    const symbol = sheet.getCell(r, 0).value?.toString().trim().toUpperCase();
    const price = priceMap[symbol];
    const cell = sheet.getCell(r, 31);
    if (symbol && price != null && cell.value !== price) {
      cell.value = parseFloat(price.toFixed(2));
      priceCount++;
    }
  }

  await sheet.saveUpdatedCells();
  console.log(`✅ Updated ${priceCount} price change rows.`);

  // Update Q2 date
  q2Cell.value = `${newDay.padStart(2, "0")}/${(monthMap[newMonStr] + 1).toString().padStart(2, "0")}/${newYear}`;
  await sheet.saveUpdatedCells();
  console.log(`📅 Q2 updated to new date: ${q2Cell.value}`);
}





async function main() {
  try {
    console.log("🟡 Starting script...");
    const urlDateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${urlDateStr}.csv`;
    const url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_06082025.csv";
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
