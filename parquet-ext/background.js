chrome.webNavigation.onBeforeNavigate.addListener(
  (details) => {
    if (details.frameId !== 0) return;
    const url = new URL(details.url);
    if (url.pathname.endsWith('.parquet')) {
      chrome.tabs.update(details.tabId, {
        url: chrome.runtime.getURL('viewer.html') + '?url=' + encodeURIComponent(details.url)
      });
    }
  }
);
