use maud::{Markup, PreEscaped, html};

use crate::explorer::components::svg_assets::{icon_check, icon_copy};

pub struct HeaderSummaryItem {
    pub label: String,
    pub value: Markup,
}

pub struct HeaderCta {
    pub label: String,
    pub href: String,
    pub external: bool,
}

pub enum HeaderPillTone {
    Success,
    Warning,
    Neutral,
}

pub struct HeaderProps {
    pub title: String,
    pub id: Option<String>,
    pub show_copy: bool,
    pub pill: Option<(String, HeaderPillTone)>,
    pub summary_items: Vec<HeaderSummaryItem>,
    pub cta: Option<HeaderCta>,
    pub hero_class: Option<String>,
}

pub fn header(props: HeaderProps) -> Markup {
    let HeaderProps { title, id, show_copy, pill, summary_items, cta, hero_class } = props;
    let has_summary = !summary_items.is_empty();
    let hero_class = match hero_class {
        Some(extra) if !extra.trim().is_empty() => format!("tx-hero {}", extra.trim()),
        _ => "tx-hero".to_string(),
    };

    let pill_markup = pill.as_ref().map(|(text, tone)| {
        let tone_class = match tone {
            HeaderPillTone::Success => "",
            HeaderPillTone::Warning => " pending",
            HeaderPillTone::Neutral => " neutral",
        };
        html! {
            span class=(format!("tx-conf-pill{}", tone_class)) { (text) }
        }
    });

    html! {
        div class=(hero_class) {
            div class="tx-hero-head" {
                div class="tx-hero-title" {
                    h1 class="h1" { (title) }
                    @if let Some(id) = id.as_ref() {
                        div class="tx-hero-id mono" {
                            span class="tx-hero-id-text" { (id) }
                            @if show_copy {
                                button class="tx-copy" type="button" data-copy-btn="" data-copy-value=(id) aria-label="Copy id" {
                                    span class="tx-copy-icon" aria-hidden="true" { (icon_copy()) }
                                    span class="tx-check-icon" aria-hidden="true" { (icon_check()) }
                                }
                            }
                        }
                    }
                }
                @if let Some(pill) = pill_markup {
                    (pill)
                }
            }
            @if has_summary {
                div class="summary-card" {
                    div class="summary-grid" {
                        @for item in summary_items {
                            div class="summary-item" {
                                span class="summary-label" { (item.label) }
                                (item.value)
                            }
                        }
                    }
                    @if let Some(cta) = cta {
                        a class="tx-mempool-link" href=(cta.href) target={(if cta.external { "_blank" } else { "_self" })} rel={(if cta.external { "noopener noreferrer" } else { "" })} {
                            (cta.label)
                            @if cta.external {
                                span class="tx-mempool-arrow" aria-hidden="true" { "↗" }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub fn header_scripts() -> Markup {
    let script = r#"
<script>
(() => {
  const buttons = document.querySelectorAll('[data-copy-btn]');
  buttons.forEach((btn) => {
    const label = btn.querySelector('[data-copy-label]');
    const value = btn.dataset.copyValue || '';
    if (!value) return;
    const markCopied = () => {
      btn.dataset.copied = '1';
      if (label) label.textContent = 'Copied';
      setTimeout(() => {
        btn.dataset.copied = '';
        if (label) label.textContent = 'Copy';
      }, 1000);
    };
    btn.addEventListener('click', async () => {
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(value);
          markCopied();
          return;
        }
      } catch (_) {
        /* noop */
      }
      const ta = document.createElement('textarea');
      ta.value = value;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand('copy');
        markCopied();
      } catch (_) {
        btn.dataset.error = '1';
      }
      ta.remove();
    });
  });
})();
(() => {
  const formatRel = (ts) => {
    const diff = Math.max(0, Date.now() / 1000 - ts);
    const mins = Math.floor(diff / 60);
    const hrs = Math.floor(mins / 60);
    const days = Math.floor(hrs / 24);
    if (days > 365) return `${Math.floor(days / 365)}y ago`;
    if (days > 30) return `${Math.floor(days / 30)}mo ago`;
    if (days > 0) return `${days}d ago`;
    if (hrs > 0) return `${hrs}h ago`;
    if (mins > 0) return `${mins}m ago`;
    return 'just now';
  };
  document.querySelectorAll('[data-ts-group]').forEach((group) => {
    const tsNode = group.querySelector('[data-header-ts]');
    if (!tsNode) return;
    const raw = Number(tsNode.dataset.headerTs);
    if (!Number.isFinite(raw)) return;
    const date = new Date(raw * 1000);
    const formatter = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' });
    tsNode.textContent = formatter.format(date);
    const relNode = group.querySelector('[data-header-ts-rel]');
    if (relNode) {
      relNode.textContent = `(${formatRel(raw)})`;
    }
  });
})();
</script>
"#;
    PreEscaped(script.to_string())
}
