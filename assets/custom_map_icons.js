// Plotly appends "-15" to layout.map.layers symbol icon names before passing to MapLibre.
// We register our PNGs under those suffixed keys using the same Image() pattern Plotly uses
// for its own Maki icons.
window.addEventListener('load', function () {
    var ICONS = [
        { id: 'dredge-icon-15',   src: '/assets/dredge_marker.png' },
        { id: 'shoaling-icon-15', src: '/assets/shoaling_marker.png' },
    ];
    var iconListenerAdded = false;

    function addIcons(map) {
        ICONS.forEach(function (icon) {
            if (map.hasImage(icon.id)) return;
            var img = new Image();
            img.onload = function () {
                if (!map.hasImage(icon.id)) {
                    map.addImage(icon.id, img);
                    map.triggerRepaint();
                }
            };
            img.crossOrigin = 'Anonymous';
            img.src = icon.src;
        });
    }

    function setup() {
        var gd = document.querySelector('#map .js-plotly-plot');
        if (!gd || !gd._fullLayout || !gd._fullLayout.map || !gd._fullLayout.map._subplot) {
            setTimeout(setup, 200);
            return;
        }
        var map = gd._fullLayout.map._subplot.map;
        if (!map) { setTimeout(setup, 200); return; }

        addIcons(map);

        if (!iconListenerAdded) {
            iconListenerAdded = true;
            map.on('styleimagemissing', function (e) {
                var match = ICONS.find(function (icon) { return icon.id === e.id; });
                if (match) addIcons(map);
            });
        }

        if (gd.on) {
            gd.on('plotly_afterplot', function () { addIcons(map); });
        }
    }

    setup();
});
