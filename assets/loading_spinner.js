// Flips the map-spinner (assets/custom.css) from black to white after the very
// first map load finishes. Black is only needed for that first load, when the
// spinner sits on the plain white page before the map has rendered behind it.
(function () {
    var sawLoading = false;
    var tries = 0;
    var maxTries = 200; // ~20s safety net in case data-dash-is-loading is missed entirely
    var interval = setInterval(function () {
        tries++;
        var mapEl = document.getElementById('map');
        if (mapEl) {
            var isLoading = mapEl.getAttribute('data-dash-is-loading') === 'true';
            if (isLoading) {
                sawLoading = true;
            } else if (sawLoading) {
                document.body.classList.add('app-loaded');
                clearInterval(interval);
                return;
            }
        }
        if (tries >= maxTries) {
            document.body.classList.add('app-loaded');
            clearInterval(interval);
        }
    }, 100);
})();
