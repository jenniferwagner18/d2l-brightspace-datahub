// Paste the following code in the browser console when logged into D2L with your admin account (if you have permission to access BDS data sets).
// Use these functions to pause, resume, or stop the download  since there are 100+ files with GBs of data:
// Downloader.pause()
// Downloader.resume()
// Downloader.stop()

window.Downloader = {
  paused: false,
  running: false,
  stopRequested: false,

  pause() {
    this.paused = true;
    console.log("⏸ Paused");
  },

  resume() {
    this.paused = false;
    console.log("▶ Resumed");
  },

  stop() {
    this.stopRequested = true;
    console.log("⛔ Stop requested");
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

    console.log(`Found ${datasets.length} datasets\n`);

    for (const wrapper of datasets) {
  if (Downloader.stopRequested) break;
  await waitIfPaused();

  const dataset = wrapper.Full || wrapper;

  const datasetName =
    dataset.Name ||
    "Unnamed Dataset";

      try {
        console.log(`Checking: ${datasetName}`);

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
