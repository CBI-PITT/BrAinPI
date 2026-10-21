(() => {
    const script = document.currentScript;
    const endpoint = script.dataset.storageUrl;
    const button = document.getElementById('storage-check');
    const label = document.getElementById('storage-check-label');
    const popover = document.getElementById('storage-popover');
    const message = document.getElementById('storage-status-message');
    const checkedAt = document.getElementById('storage-checked-at');
    const container = button.closest('.storage-nav');
    const formatBytes = (bytes) => {
        const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
        let index = 0;
        while (bytes >= 1024 && index < units.length - 1) {
            bytes /= 1024;
            index += 1;
        }
        return `${bytes.toFixed(index ? 2 : 0)} ${units[index]}`;
    };
    const showUsage = (kind, data) => {
        const configured = data.configured !== false;
        document.getElementById(`storage-${kind}-result`).textContent = configured ? formatBytes(data.used_bytes) : 'Disabled';
        document.getElementById(`storage-${kind}-limit`).textContent = !configured ? 'Cache is not configured' :
            data.limit_bytes ? `of ${formatBytes(data.limit_bytes)} limit` : 'No storage limit';
        const percent = configured && data.limit_bytes ? Math.min(100, data.used_bytes / data.limit_bytes * 100) : 0;
        document.getElementById(`storage-${kind}-bar`).style.width = `${percent}%`;
    };
    const close = () => {
        popover.hidden = true;
        button.setAttribute('aria-expanded', 'false');
    };
    document.addEventListener('click', (event) => {
        if (!container.contains(event.target)) close();
    });
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && !popover.hidden) {
            close();
            button.focus();
        }
    });
    // One click, one request for both stores. No automatic scans or retries.
    button.addEventListener('click', async () => {
        popover.hidden = false;
        button.setAttribute('aria-expanded', 'true');
        button.disabled = true;
        label.textContent = 'Checking…';
        message.textContent = 'Checking cache and pyramid storage…';
        message.dataset.error = 'false';
        try {
            const url = new URL(endpoint, window.location.origin);
            const response = await fetch(url, {cache: 'no-store', credentials: 'same-origin'});
            if (!response.ok) {
                const error = await response.json().catch(() => ({}));
                throw new Error(error.error || 'Storage check failed. Please retry.');
            }
            const data = await response.json();
            showUsage('cache', data.cache);
            showUsage('pyramid', data.pyramid);
            checkedAt.textContent = `Checked ${new Date(data.checked_at * 1000).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'})}`;
            message.textContent = data.pyramid.cleanup_pending ? 'Pyramid cleanup is pending.' :
                'Click Storage again to refresh.';
        } catch (error) {
            message.textContent = error.message;
            message.dataset.error = 'true';
        } finally {
            button.disabled = false;
            label.textContent = 'Storage';
        }
    });
})();
