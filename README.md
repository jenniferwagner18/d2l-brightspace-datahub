# Download D2L Brightspace Data Sets and Build a DuckDB Database

Use the Javascript to automate downloading all or listed full Brightspace data sets through the browser when logged in with your admin account - no need for OAuth 2.0, access tokens, or downloading files manually via Data Hub.

Once the ZIP files are saved to your hard drive, use Python to build your own database with the DuckDB database management system. Re-build your database whenever you download updated files and query your database using SQL with output to CSV.

You can also compare the data set files saved in two folders from different sources, such as a data warehouse and the Data Hub. This will generate a summary of row counts and diff files, and optionally extract the data set ZIP files to CSV.

## Command Line Options
If you just need row counts of downloaded zip files, open Terminal on a Mac, navigate to the folder using **cd *foldername*** and paste this:

`for f in *.zip; do echo -n "$f: "; echo $(($(unzip -p "$f" | wc -l) - 1)); done`

You can also unzip all files in the folder with:

`for f in *.zip; do unzip -jo "$f" "*.csv" -d.; done`
