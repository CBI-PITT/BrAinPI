(() => {
    const retry = document.getElementById('maintenance-retry');
    retry.addEventListener('click', () => {
        // Navigate with GET rather than replaying a blocked form submission.
        window.location.assign(window.location.href);
    });
})();
