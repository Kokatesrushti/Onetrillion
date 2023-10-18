const express = require('express');
const mysql = require('mysql');
const bodyParser = require('body-parser');
const puppeteer = require('puppeteer');
const moment = require('moment');
const multer = require('multer');
const cors = require('cors');
const fs = require('fs');
const csv = require('csv-parser'); // Add csv-parser for processing CSV files
const { Readable } = require('stream'); // Import Readable from stream module

const app = express();
const port = 5000;

app.use(cors());
app.use(bodyParser.json());

// Define a multer storage for file uploads
const storage = multer.memoryStorage();
const upload = multer({ storage });

// MySQL database connection configuration
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'Saurabh2004',
  database: 'onetrillion',
});

connection.connect((err) => {
  if (err) {
    console.error('Error connecting to MySQL database:', err);
    return;
  }
  console.log('Connected to MySQL database');
});



app.post('/login', (req, res) => {
  const { username, password } = req.body;
  const query = 'SELECT * FROM users WHERE username = ? AND password = ?';

  connection.query(query, [username, password], (err, result) => {
    if (err) {
      console.error('Error executing MySQL query:', err);
      return res.status(500).json({ message: 'Login failed' });
    }

    if (result.length > 0) {
      return res.json({ message: 'Login successful' });
    } else {
      return res.status(401).json({ message: 'Login failed' });
    }
  });
});

// Web scraping function
async function scrapeAndStore(csvData) {
  const browser = await puppeteer.launch({
    headless: false, // Set to true for production
    defaultViewport: null,
    userDataDir: './tmp',
  });

  const page = await browser.newPage();

  for (let i = 0; i < csvData.length; i++) {
    const searchQuery = csvData[i].query;
    const asinNo = csvData[i].asin_no;
    const activeStatus = csvData[i].active_status;

    // Check if the active status is "active" before searching
    if (activeStatus === 'active') {
      const url = `https://www.amazon.in/s?k=${searchQuery}&ref=nb_sb_noss`;
      await page.goto(url);
      let page_num = 1;
      let isBtnDisabled = false;

      let asinFound = false;
      let sponsoredPosition = 1;
      let organicPosition = null;

      while (!isBtnDisabled) {
        await page.waitForSelector('[data-cel-widget="search_result_0"]');
        const productsHandles = await page.$$(
          'div.s-main-slot.s-result-list.s-search-results.sg-row > .s-result-item'
        );

        let organicProductFound = false;

        for (const producthandle of productsHandles) {
          let title = 'Null';
          let sp = 'Null';
          let asin = 'Null';
          try {
            sp = await page.evaluate(
              (el) =>
                el.querySelector(
                  ' a > span.puis-label-popover-default > span'
                ).textContent,
              producthandle
            );
          } catch (error) {}
          try {
            title = await page.evaluate(
              (el) => el.querySelector('h2 > a > span').textContent,
              producthandle
            );
          } catch (error) {}
          try {
            asin = await page.evaluate(
              (el) => el.getAttribute('data-asin'),
              producthandle
            );
          } catch (error) {}

          if (title !== 'Null') {
            if (sp !== 'Sponsored') {
              organicPosition = organicPosition || 1;

              // Insert data into MySQL
              const query = `INSERT INTO new_table (searchQuery, asin, page_num, sponsored_position, organic_position, title, created_at) VALUES (?, ?, ?, ?, ?, ?, NOW())`;
              const values = [
                searchQuery,
                asin,
                page_num,
                null,
                organicPosition,
                title.replace(/,/g, '.'),
              ];

              try {
                await connection.query(query, values);
                console.log('Data inserted successfully!');
              } catch (error) {
                if (error.code === 'ER_DUP_ENTRY') {
                  console.log('Skipping duplicate record:', values);
                } else {
                  throw error;
                }
              }

              fs.appendFile(
                'results.csv',
                `${page_num},${organicPosition},${title.replace(
                  /,/g,
                  '.'
                )},${asin}\n`,
                function (err) {
                  if (err) {
                    console.log('Error writing to file:', err);
                    throw err;
                  }
                }
              );

              organicPosition++;
              organicProductFound = true;
            } else {
              const query = `INSERT INTO new_table (searchQuery, asin, page_num, sponsored_position, organic_position, title, created_at) VALUES (?, ?, ?, ?, ?, ?, NOW())`;
              const values = [
                searchQuery,
                asin,
                page_num,
                sponsoredPosition,
                null,
                title.replace(/,/g, '.'),
              ];

              try {
                await connection.query(query, values);
                console.log('Data inserted successfully!');
              } catch (error) {
                if (error.code === 'ER_DUP_ENTRY') {
                  console.log('Skipping duplicate record:', values);
                } else {
                  throw error;
                }
              }

              fs.appendFile(
                'results.csv',
                `${page_num},${sponsoredPosition},${title.replace(
                  /,/g,
                  '.'
                )},${asin}\n`,
                function (err) {
                  if (err) {
                    console.log('Error writing to file:', err);
                    throw err;
                  }
                }
              );

              sponsoredPosition++;
            }
          }

          if (asinNo === asin) {
            asinFound = true;
            const currentDateTime = moment().format('YYYY-MM-DD HH:mm:ss');
            connection.query(
              `UPDATE search_queries SET active_status = 'active', last_crawled = '${currentDateTime}', sponsored_position = ${sponsoredPosition}, organic_position = ${organicPosition}, page_num = ${page_num} WHERE id = ${csvData[i].id}`,
              (error, results, fields) => {
                if (error) throw error;
              }
            );
            connection.query(
              `UPDATE new_table SET matched = 'true' WHERE asin = '${asinNo}'`,
              (error, results, fields) => {
                if (error) throw error;
              }
            );
          }
        }

        if (!asinFound && !organicProductFound) {
          const currentDateTime = moment().format('YYYY-MM-DD HH:mm:ss');
          connection.query(
            `UPDATE search_queries SET active_status = 'not active', last_crawled = '${currentDateTime}' WHERE id = ${csvData[i].id}`,
            (error, results, fields) => {
              if (error) throw error;
            }
          );
        }

        await page.waitForSelector('.s-pagination-item.s-pagination-next', {
          visible: true,
        });
        const is_disabled =
          (await page.$(
            '.span.s-pagination-item.s-pagination-next.s-pagination-disabled'
          )) !== null;

        isBtnDisabled = is_disabled;
        if (!is_disabled) {
          page_num++;
          await Promise.all([
            page.click('.s-pagination-item.s-pagination-next'),
            page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 60000 }),
          ]);
        }
      }
    }
  }

  await browser.close();
}



app.post('/upload', upload.single('csvFile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ message: 'No file uploaded' });
  }

  const fileData = req.file.buffer.toString('utf8');
  const results = [];


  const stream = Readable.from(fileData);
  stream
    .pipe(csv())
    .on('data', (data) => {
      results.push(data);
    })
    .on('end', async () => {
      try {
        // Call the web scraping function with the parsed CSV data
        await scrapeAndStore(results);

        res.json({ message: 'File uploaded and processed successfully', data: results });
      } catch (error) {
        console.error('Error during scraping and storing:', error);
        res.status(500).json({ message: 'File processing failed' });
      }
    });
});



app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
