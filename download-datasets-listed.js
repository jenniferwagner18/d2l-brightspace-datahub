/* Download only the FULL Brightspace datasets listed as TARGET_DATASETS

Change your browser settings to specify in which folder to save the files.

Paste the following in the browser console when logged into D2L with admin account 
OR create a bookmark in your browser and replace the URL with: 
javascript:(async()=>{ paste all the code below here })();

While downloading, paste in these functions in the console to pause, resume, 
or stop the download (if needed):

Downloader.pause()
Downloader.resume()
Downloader.stop()
*/

window.Downloader = {
  paused: false,
  running: false,
  stopRequested: false,

  pause() {
    this.paused = true;
    console.log("Paused");
  },

  resume() {
    this.paused = false;
    console.log("Resumed");
  },

  stop() {
    this.stopRequested = true;
    console.log("Stop requested");
  },

  status() {
    console.log({
      running: this.running,
      paused: this.paused,
      stopRequested: this.stopRequested
    });
  }
};

(async () => {

  const API_VERSION = "1.49";
  const BASE = `/d2l/api/lp/${API_VERSION}/datasets/bds`;
  const DELAY_MS = 1500;

  // specify data sets to download here:
  const TARGET_DATASETS = new Set([
    "Content Objects",
    "Content User Progress",
    "Assignment Submissions",
    "Assignment Summary",
    "Quiz Attempts",
    "Quiz Objects",
    "Discussion Posts",
    "Discussion Topics",
    "Grade Objects",
    "Grade Results",
    "Course Access",
    "Course Access Log",
    "Organizational Units",
    "Users",
    "User Enrollments"
  ]);

  function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
  }

  function unwrap(result) {
    if (Array.isArray(result)) return result;
    if (result?.Items) return result.Items;
    if (result?.Objects) return result.Objects;
    return [];
  }

  function triggerDownload(url) {
    const a = document.createElement("a");
    a.href = url;
    a.download = "";
    document.body.appendChild(a);
    a.click();
    a.remove();
  }

  async function waitIfPaused() {
    while (Downloader.paused) {
      await sleep(500);
    }
  }

  try {

    Downloader.running = true;
    Downloader.stopRequested = false;

    console.log("Fetching dataset list...");

    const datasetsRes = await fetch(BASE, { credentials: "same-origin" });
    const datasetsJson = await datasetsRes.json();
    const datasets = unwrap(datasetsJson);

    if (!datasets.length) {
      console.error("No datasets found. Response was:", datasetsJson);
      Downloader.running = false;
      return;
    }

    console.log(`Found ${datasets.length} datasets`);

    const filteredDatasets = datasets.filter(wrapper => {
      const dataset = wrapper.Full || wrapper;
      return TARGET_DATASETS.has(dataset.Name);
    });

    console.log(`Filtering to ${filteredDatasets.length} selected datasets\n`);

    let index = 0;

    for (const wrapper of filteredDatasets) {

      index++;

      if (Downloader.stopRequested) break;

      await waitIfPaused();

      const dataset = wrapper.Full || wrapper;

      const datasetName =
        dataset.Name ||
        "Unnamed Dataset";

      try {

        console.log(`[${index} / ${filteredDatasets.length}] Checking: ${datasetName}`);

        if (!dataset.ExtractsLink) {
          console.log("  No ExtractsLink found\n");
          continue;
        }

        const extractsRes = await fetch(dataset.ExtractsLink, {
          credentials: "same-origin"
        });

        const extractsJson = await extractsRes.json();
        const extracts = unwrap(extractsJson);

        if (!extracts.length) {
          console.log("  No extracts available\n");
          continue;
        }

        const fullExtracts = extracts
          .filter(e => e.BdsType === "Full")
          .sort((a, b) =>
            new Date(b.CreatedDate) - new Date(a.CreatedDate)
          );

        if (!fullExtracts.length) {
          console.log("  No FULL extracts found\n");
          continue;
        }

        const latest = fullExtracts[0];

        if (!latest.DownloadLink) {
          console.log("  FULL extract has no DownloadLink\n");
          continue;
        }

        console.log(`  Downloading FULL from ${latest.CreatedDate}`);

        triggerDownload(latest.DownloadLink);

        await sleep(DELAY_MS);

      } catch (err) {

        console.error(`  Error processing ${datasetName}:`, err);

      }
    }

    console.log("\nDone.");

  } catch (err) {

    console.error("Fatal error:", err);

  } finally {

    Downloader.running = false;

  }

})();
