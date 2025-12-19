use maud::{Markup, PreEscaped, html};

/// Carousel of blocks around the current height. Uses Embla (CDN) for smooth drag/scroll.
pub fn block_carousel(current_height: Option<u64>, espo_tip: u64) -> Markup {
    let Some(current_height) = current_height else {
        return html! {};
    };

    let script = PreEscaped(format!(
        r#"
(function() {{
  const root = document.querySelector('[data-block-carousel]');
  if (!root) return;
  const viewport = root.querySelector('[data-bc-viewport]');
  const container = root.querySelector('[data-bc-container]');
  const current = Number(root.dataset.current);
  const espoTip = Number(root.dataset.espoTip);
  if (!viewport || !container || !Number.isFinite(current) || !Number.isFinite(espoTip)) return;

  let minH = current;
  let maxH = current;
  const seen = new Set();
  const blocks = [];
  let fetching = false;
  let embla = null;
  let loadingLeft = false;
  let loadingRight = false;
  let selectedHeight = current;
  let pendingEdge = false;
  let leftDepleted = false;
  let rightDepleted = false;
  let suppressSelectUpdate = false;
  let scrollRaf = null;
  let hasCentered = false;
  const isLatest = current === espoTip;
  let indicator = null;

  const setLoading = (side, val) => {{
    if (side === 'left') {{
      loadingLeft = val;
      root.dataset.loadingleft = val ? '1' : '0';
    }} else {{
      loadingRight = val;
      root.dataset.loadingright = val ? '1' : '0';
    }}
  }};

  function formatAgo(ts) {{
    if (!ts) return '';
    const diff = Math.max(0, Date.now() / 1000 - ts);
    const mins = Math.floor(diff / 60);
    const hrs = Math.floor(mins / 60);
    const days = Math.floor(hrs / 24);
    if (days > 365) return `${{Math.floor(days/365)}}y ago`;
    if (days > 30) return `${{Math.floor(days/30)}}mo ago`;
    if (days > 0) return `${{days}}d ago`;
    if (hrs > 0) return `${{hrs}}h ago`;
    if (mins > 0) return `${{mins}}m ago`;
    return 'just now';
  }}

  function ensureIndicator() {{
    if (indicator) return indicator;
    indicator = root.querySelector('[data-bc-indicator]');
    if (!indicator) {{
      indicator = document.createElement('div');
      indicator.className = 'bc-indicator';
      indicator.dataset.bcIndicator = '1';
      indicator.setAttribute('aria-hidden', 'true');
      indicator.innerHTML = '<svg class="bc-indicator-svg" viewBox="0 0 24 14" aria-hidden="true" focusable="false"><path d="M12 14L0 0h24L12 14z"></path></svg>';
    }}
    return indicator;
  }}

  function getIndexByHeight(height) {{
    return blocks.findIndex((b) => b.height === height);
  }}

  function snapToHeight(height, jump = false) {{
    if (!embla) return;
    const idx = getIndexByHeight(height);
    if (idx < 0) return;
    embla.scrollTo(idx, jump);
  }}

  function updateIndicator() {{
    if (!embla) return;
    const node = ensureIndicator();
    if (!node) return;
    const currentCard = container.querySelector('.bc-card.current');
    if (!currentCard) {{
      node.style.opacity = '0';
      return;
    }}
    node.style.opacity = '1';
    currentCard.appendChild(node);
  }}

  function snapBackIfLatest() {{
    if (!isLatest || !embla) return;
    const currentCard = container.querySelector('.bc-card.current');
    if (!currentCard) return;
    const viewportRect = viewport.getBoundingClientRect();
    const cardRect = currentCard.getBoundingClientRect();
    const viewportCenter = viewportRect.left + viewportRect.width / 2;
    const cardCenter = cardRect.left + cardRect.width / 2;
    if (cardCenter < viewportCenter - 1) {{
      snapToHeight(current, false);
    }}
  }}

  function render() {{
    if (embla) {{
      const idx = embla.selectedScrollSnap();
      const slide = embla.slideNodes()[idx];
      if (slide && slide.dataset.height) {{
        selectedHeight = Number(slide.dataset.height);
      }}
    }}
    blocks.sort((a,b) => a.height - b.height);
    container.innerHTML = '';
    for (const b of blocks) {{
      const slide = document.createElement('div');
      slide.className = 'bc-slide';
      slide.dataset.height = String(b.height);
      slide.innerHTML = `
        <div class="bc-top">
          <span class="bc-height-tag">${{b.height}}</span>
        </div>
        <a class="bc-card${{b.height === current ? ' current' : ''}}" href="/block/${{b.height}}">
          <div class="bc-face">
            <div class="bc-traces">${{b.traces}} traces</div>
            <div class="bc-time">${{formatAgo(b.time)}}</div>
          </div>
        </a>
      `;
      container.appendChild(slide);
    }}

    let targetIdx = getIndexByHeight(hasCentered ? selectedHeight : current);
    if (targetIdx < 0) {{
      targetIdx = blocks.findIndex((b) => b.height === current);
    }}
    if (targetIdx < 0) targetIdx = 0;

    const opts = {{
      align: 'center',
      dragFree: true,
      containScroll: false,
      startIndex: targetIdx
    }};

    const ensureSelectedHeight = () => {{
      if (suppressSelectUpdate) return;
      if (!embla) return;
      const idx = embla.selectedScrollSnap();
      const slide = embla.slideNodes()[idx];
      if (slide && slide.dataset.height) {{
        selectedHeight = Number(slide.dataset.height);
      }}
    }};

    suppressSelectUpdate = true;
    if (!embla && window.EmblaCarousel) {{
      embla = window.EmblaCarousel(viewport, opts);
      embla.on('select', () => {{
        ensureSelectedHeight();
        updateIndicator();
        maybeLoadMore();
      }});
      embla.on('scroll', throttleMaybeLoad);
      embla.on('settle', snapBackIfLatest);
      embla.on('pointerUp', snapBackIfLatest);
    }} else if (embla) {{
      embla.reInit(opts);
    }}

    if (embla) {{
      const idx = targetIdx;
      window.requestAnimationFrame(() => {{
        embla.scrollTo(idx, true);
        hasCentered = true;
        suppressSelectUpdate = false;
        ensureSelectedHeight();
        updateIndicator();
        snapBackIfLatest();
      }});
    }}
    suppressSelectUpdate = false;
    maybeLoadMore();
  }}

  async function fetchAround(center, dir = 0) {{
    if (fetching) {{
      pendingEdge = true;
      return;
    }}
    if (center < 0 || center > espoTip) return;
    fetching = true;
    if (dir < 0) setLoading('left', true);
    if (dir > 0) setLoading('right', true);
    try {{
      const res = await fetch(`/api/blocks/carousel?center=${{center}}&radius=8`);
      if (!res.ok) return;
      const data = await res.json();
      if (!data || !Array.isArray(data.blocks)) return;
      let added = false;
      for (const b of data.blocks) {{
        if (b.height > espoTip) continue;
        if (seen.has(b.height)) continue;
        seen.add(b.height);
        blocks.push(b);
        minH = Math.min(minH, b.height);
        maxH = Math.max(maxH, b.height);
        added = true;
      }}
      if (!added) {{
        if (dir < 0) {{
          minH = 0;
          leftDepleted = true;
        }} else if (dir > 0) {{
          maxH = espoTip;
          rightDepleted = true;
        }}
      }} else {{
        if (dir < 0) leftDepleted = false;
        if (dir > 0) rightDepleted = false;
      }}
      render();
    }} finally {{
      fetching = false;
      if (dir < 0) setLoading('left', false);
      if (dir > 0) setLoading('right', false);
      if (pendingEdge) {{
        pendingEdge = false;
        maybeLoadMore();
      }}
    }}
  }}

  function maybeLoadMore() {{
    if (!embla || blocks.length === 0) return;
    const inView = embla.slidesInView(false);
    let minIdx = embla.selectedScrollSnap();
    let maxIdx = minIdx;
    if (inView && inView.length) {{
      minIdx = Math.min(...inView);
      maxIdx = Math.max(...inView);
    }}
    let progress = null;
    if (typeof embla.scrollProgress === 'function') {{
      progress = embla.scrollProgress();
    }}
    const clamped = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : null;
    const nearLeft = minIdx <= 2 || (clamped !== null && clamped <= 0.03);
    const nearRight = maxIdx >= blocks.length - 3 || (clamped !== null && clamped >= 0.97);
    if (nearLeft && minH > 0 && !leftDepleted) {{
      fetchAround(Math.max(0, minH - 8), -1);
    }}
    if (nearRight && maxH < espoTip && !rightDepleted) {{
      fetchAround(Math.min(espoTip, maxH + 8), 1);
    }}
  }}

  function throttleMaybeLoad() {{
    if (scrollRaf) return;
    scrollRaf = window.requestAnimationFrame(() => {{
      scrollRaf = null;
      maybeLoadMore();
      updateIndicator();
    }});
  }}

  function ensureEmbla(cb) {{
    if (window.EmblaCarousel) return cb();
    let script = document.getElementById('embla-cdn');
    if (!script) {{
      script = document.createElement('script');
      script.id = 'embla-cdn';
      script.src = 'https://unpkg.com/embla-carousel/embla-carousel.umd.js';
      script.async = true;
      document.head.appendChild(script);
    }}
    script.addEventListener('load', cb, {{ once: true }});
  }}

  ensureEmbla(() => fetchAround(current));
}})();
"#
    ));

    html! {
        div class="block-carousel card full-bleed" data-block-carousel data-current=(current_height) data-espo-tip=(espo_tip) {
            div class="bc-inner" {

                div class="bc-embla-wrap" {
                    div class="bc-embla" data-bc-viewport {
                        div class="bc-embla__container" data-bc-container {
                            div class="muted" { "Loading blocks…" }
                        }
                    }
                    div class="bc-loader left" aria-hidden="true" { div class="spinner" {} }
                    div class="bc-loader right" aria-hidden="true" { div class="spinner" {} }
                }
            }
        }
        script { (script) }
    }
}
