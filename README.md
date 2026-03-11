# Download D2L Brightspace Data Sets and Build a DuckDB Database

Use the Javascript to automate downloading all or listed full Brightspace data sets through the browser when logged in with your admin account - no need for OAuth 2.0, access tokens, or downloading files manually via Data Hub.

Once the ZIP files are saved to your hard drive, use Python to build your own database with the DuckDB database management system. Re-build your database whenever you download updated files and query your database using SQL with output to CSV. Each rebuild will also provide the record counts for all tables in the database. (This is a mirrored database rather than historical.)

You can also compare the data set files saved in two folders from different sources, such as a data warehouse and the Data Hub. This will generate a summary of record counts and diff files, and optionally extract the data set ZIP files to CSV.

## Command Line Alternatives
If you just need record counts of downloaded zip files or want to unzip all files in the folder, open Terminal on a Mac, navigate to the folder using **cd *foldername*** and use the following commands. For Windows, try Windows Subsystem for Linux. Note that counting records via command line is quite a bit slower compared to using DuckDB, but these commands do not require Python to be installed.

`for f in *.zip; do printf "%s: " "$f"; unzip -p "$f" "*.csv" | awk '{line=$0; gsub(/""/,"",line); n=gsub(/"/,"&",line); if(!inquote) rows++; if(n%2==1) inquote=!inquote} END{print rows-1}'; done`

You can also unzip all files in the folder with:

`for f in *.zip; do unzip -jo "$f" "*.csv" -d.; done`
