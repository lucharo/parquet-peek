// Track which tab initiated each .parquet navigation (DownloadItem has no tabId)
const pendingParquetTabs = new Map();

function getViewerUrl(fileUrl) {
  return chrome.runtime.getURL('viewer.html') + '?url=' + encodeURIComponent(fileUrl);
}

// Layer 1: Redirect .parquet navigations to the viewer
chrome.webNavigation.onBeforeNavigate.addListener((details) => {
  if (details.frameId !== 0) return;
  if (!/\.parquet([?#]|$)/i.test(details.url)) return;

  pendingParquetTabs.set(details.url, details.tabId);
  setTimeout(() => pendingParquetTabs.delete(details.url), 30000);

  chrome.tabs.update(details.tabId, { url: getViewerUrl(details.url) });
});

// Layer 2: Catch .parquet downloads that slip past declarativeNetRequest
// (server sends Content-Disposition: attachment before redirect takes effect)
chrome.downloads.onCreated.addListener((downloadItem) => {
  const url = downloadItem.url || downloadItem.finalUrl;
  if (!url || !/\.parquet([?#]|$)/i.test(url)) return;

  chrome.downloads.cancel(downloadItem.id, () => {
    chrome.downloads.erase({ id: downloadItem.id });
  });

  const viewerUrl = getViewerUrl(url);
  const tabId = pendingParquetTabs.get(url);
  pendingParquetTabs.delete(url);

  if (tabId) {
    chrome.tabs.update(tabId, { url: viewerUrl });
  } else {
    chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
      if (tabs[0]) {
        chrome.tabs.update(tabs[0].id, { url: viewerUrl });
      } else {
        chrome.tabs.create({ url: viewerUrl });
      }
    });
  }
});
