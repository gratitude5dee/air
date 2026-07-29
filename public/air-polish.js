(() => {
  const STYLE_ID = "air-polish-styles";
  const CURSOR_ID = "air-page-cursor";
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  const finePointer = window.matchMedia("(pointer: fine)");

  function installStyles() {
    if (document.getElementById(STYLE_ID)) return;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = `
      header[data-header] a[aria-label="Air by WZRD.tech"] {
        gap: 0 !important;
        height: 3.9rem !important;
        min-height: 3.9rem !important;
        padding: 0 1.4rem !important;
        border: 1px solid rgba(255,255,255,0.14) !important;
        border-radius: 999px !important;
        background: rgba(12,11,16,0.62) !important;
        box-shadow: 0 0.5rem 1.3rem rgba(2,5,10,0.4), inset 0 1px 0 rgba(255,255,255,0.08) !important;
        backdrop-filter: blur(16px) saturate(160%);
        -webkit-backdrop-filter: blur(16px) saturate(160%);
      }
      header[data-header] a[aria-label="Air by WZRD.tech"] > span { display: none !important; }
      header[data-header] a[aria-label="Air by WZRD.tech"] > img { height: 1.9rem !important; }
      [data-air-hero-mark] {
        margin: 0 0 0.95rem;
        color: #f8fbff;
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "Helvetica Neue", sans-serif;
        font-size: clamp(2.35rem, 4.35vw, 4.9rem);
        font-weight: 700;
        letter-spacing: -0.072em;
        line-height: 0.9;
        text-shadow: 0 0.75rem 2rem rgba(2,8,20,0.32);
      }
      [data-air-cursor-field] {
        position: fixed;
        top: 0;
        left: 0;
        z-index: 49;
        width: 1px;
        height: 1px;
        pointer-events: none;
        opacity: 0;
        transform: translate3d(-100px, -100px, 0);
        transition: opacity 180ms ease;
        mix-blend-mode: screen;
      }
      [data-air-cursor-field][data-active="true"] { opacity: 1; }
      [data-air-cursor-orb] {
        position: absolute;
        width: 4.75rem;
        height: 4.75rem;
        transform: translate(-50%, -50%);
        border: 1px solid rgba(214,239,255,0.72);
        border-radius: 50%;
        background: radial-gradient(circle, rgba(227,248,255,0.75) 0 5%, rgba(112,192,255,0.25) 24%, rgba(32,119,255,0.12) 42%, transparent 72%);
        box-shadow: 0 0 1.4rem rgba(119,200,255,0.65), inset 0 0 1rem rgba(231,249,255,0.28);
      }
      [data-air-cursor-orb]::after {
        content: "";
        position: absolute;
        inset: 0.48rem;
        border: 1px dashed rgba(223,246,255,0.62);
        border-radius: inherit;
        animation: air-cursor-spin 7s linear infinite;
      }
      [data-air-cursor-trail] {
        position: absolute;
        width: 1.2rem;
        height: 1.2rem;
        transform: translate(-50%, -50%);
        border-radius: 50%;
        background: #9dddff;
        box-shadow: 0 0 1.4rem 0.35rem rgba(52,144,255,0.42);
      }
      @keyframes air-cursor-spin { to { transform: rotate(360deg); } }
      @media (prefers-reduced-motion: reduce), (pointer: coarse) {
        [data-air-cursor-field] { display: none !important; }
      }
    `;
    document.head.append(style);
  }

  function installHeroMark() {
    const heading = document.getElementById("air-hero-title");
    if (!heading || heading.parentElement?.querySelector("[data-air-hero-mark]")) return;
    const mark = document.createElement("p");
    mark.dataset.airHeroMark = "";
    mark.setAttribute("aria-hidden", "true");
    mark.textContent = "air";
    heading.before(mark);
  }

  function installCursor() {
    if (document.getElementById(CURSOR_ID) || reducedMotion.matches || !finePointer.matches) return;
    const cursor = document.createElement("div");
    cursor.id = CURSOR_ID;
    cursor.dataset.airCursorField = "";
    cursor.setAttribute("aria-hidden", "true");
    cursor.innerHTML = '<span data-air-cursor-trail></span><span data-air-cursor-orb></span>';
    document.body.append(cursor);

    const trail = cursor.querySelector("[data-air-cursor-trail]");
    let targetX = -100;
    let targetY = -100;
    let trailX = -100;
    let trailY = -100;
    let frame = 0;
    let hideTimer = 0;

    const paint = () => {
      frame = 0;
      trailX += (targetX - trailX) * 0.18;
      trailY += (targetY - trailY) * 0.18;
      cursor.style.transform = `translate3d(${targetX}px, ${targetY}px, 0)`;
      if (trail) trail.style.transform = `translate3d(${trailX - targetX}px, ${trailY - targetY}px, 0) translate(-50%, -50%)`;
      if (Math.abs(targetX - trailX) > 0.15 || Math.abs(targetY - trailY) > 0.15) frame = requestAnimationFrame(paint);
    };
    const move = (event) => {
      if (event.pointerType === "touch") return;
      targetX = event.clientX;
      targetY = event.clientY;
      if (trailX === -100) {
        trailX = targetX;
        trailY = targetY;
      }
      window.clearTimeout(hideTimer);
      cursor.dataset.active = "true";
      if (!frame) frame = requestAnimationFrame(paint);
    };
    const leave = () => {
      hideTimer = window.setTimeout(() => { cursor.dataset.active = "false"; }, 260);
    };
    document.addEventListener("pointermove", move, { passive: true });
    document.addEventListener("pointerleave", leave, { passive: true });
  }

  function install() {
    installStyles();
    installHeroMark();
    installCursor();
  }

  const observer = new MutationObserver(install);
  observer.observe(document.documentElement, { childList: true, subtree: true });
  install();
})();
