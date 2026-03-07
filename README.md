# Download Brightspace Data Sets and Build a DuckDB Database

Use the Javascript to automate downloading all or listed full Brightspace data sets through the browser when logged in with your admin account - no need for OAuth 2.0, access tokens, or downloading files manually via Data Hub.

Once the ZIP files are saved to your hard drive, use Python to create tables using the DuckDB database management system. Re-build your database whenever you download updated files and query your database with SQL, with output to CSV.

You can also count rows in each table, compare the tables to external files saved to a folder, or simply unzip all the downloaded files in case you need to work with the CSV files instead of the database.
