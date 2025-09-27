# LIDOM Live (demo)

Plantilla para publicar un feed JSON y una vista estática (GitHub Pages) con información de **Pelota Invernal** (LIDOM).

## Cómo funciona

- `scraper.mjs` descarga `https://pelotainvernal.com/liga/dominicana-lidom`, extrae el primer argumento de `new ViewModel(...)` y escribe `docs/latest.json`.
- Un GitHub Action corre cada 2 minutos y comitea si hay cambios.
- `docs/index.html` muestra el scoreboard desde `latest.json` y ofrece **modo LIVE** con el WebSocket público del sitio original (demo).

## Deploy rápido

1. Sube estos archivos a un repo público.
2. Activa **GitHub Pages** apuntando a `docs/`.
3. Activa **Actions** y corre el workflow `scrape-lidom`.
