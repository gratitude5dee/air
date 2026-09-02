(() => {
  const STYLE_ID = "air-polish-styles";
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  const mobileViewport = window.matchMedia("(max-width: 767px)");

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
      [data-air-motion-section] {
        position: relative;
        isolation: isolate;
      }
      [data-air-page-bubble] {
        position: absolute;
        inset: 0;
        z-index: 0;
        display: block;
        opacity: 0.25;
        pointer-events: none;
        mix-blend-mode: screen;
      }
      [data-air-motion-section] > :not([data-air-page-bubble]) {
        position: relative;
        z-index: 1;
      }
      [data-air-launch-section] {
        position: relative;
        overflow: hidden;
        background: #05070a;
        padding: clamp(1.5rem, 4vw, 3.5rem) max(1.5rem, calc((100vw - 76rem) / 2));
      }
      [data-air-launch-shell] {
        overflow: hidden;
        border: 1px solid rgba(219, 237, 255, 0.22);
        border-radius: 1.5rem;
        background: #02060d;
        box-shadow: 0 1.5rem 3.5rem rgba(0, 0, 0, 0.3);
      }
      [data-air-launch-video] {
        display: block;
        width: 100%;
        aspect-ratio: 16 / 9;
        background: #02060d;
        object-fit: contain;
      }
      @media (max-width: 767px) {
        [data-air-launch-section] { padding: 1rem 1rem 2.5rem; }
        [data-air-launch-shell] { border-radius: 1rem; }
      }
      @media (prefers-reduced-motion: reduce) {
        [data-air-page-bubble] { display: none !important; }
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

  function installLaunchVideo() {
    const hero = document.getElementById("top");
    if (!hero || hero.parentElement?.querySelector("[data-air-launch-section]")) return;

    const section = document.createElement("section");
    section.dataset.airLaunchSection = "";
    section.setAttribute("aria-label", "Air launch video");

    const shell = document.createElement("div");
    shell.dataset.airLaunchShell = "";
    const video = document.createElement("video");
    video.dataset.airLaunchVideo = "";
    video.src = "/air-launch.mp4";
    video.controls = true;
    video.playsInline = true;
    video.preload = "metadata";
    video.setAttribute("aria-label", "Air launch video");
    shell.append(video);
    section.append(shell);
    hero.after(section);
  }

  function configureMobileIntroVideo() {
    document.querySelectorAll("[data-clip], #top video").forEach((video) => {
      if (!(video instanceof HTMLVideoElement) || video.dataset.airLaunchVideo !== undefined) return;
      video.dataset.airIntroVideo = "";

      const stopPlayback = () => {
        if (!mobileViewport.matches) return;
        video.pause();
        video.autoplay = false;
        video.removeAttribute("autoplay");
      };

      stopPlayback();
      if (video.dataset.airMobilePlaybackBound === undefined) {
        video.dataset.airMobilePlaybackBound = "";
        video.addEventListener("play", stopPlayback);
      }
    });
  }

  function installBackgroundMotion() {
    if (reducedMotion.matches) return;
    document.querySelectorAll("main > section:not(#top)").forEach((section, index) => {
      if (section.querySelector(":scope > [data-air-page-bubble]")) return;
      section.dataset.airMotionSection = "";
      const bubble = document.createElement("dk-bubble");
      bubble.dataset.airPageBubble = "";
      bubble.setAttribute("from", "blue");
      bubble.setAttribute("fade", "");
      bubble.setAttribute("pixel", index % 2 === 0 ? "3" : "4");
      bubble.setAttribute("size", index % 2 === 0 ? "28" : "24");
      bubble.setAttribute("trail", "12");
      bubble.setAttribute("bloom", "low");
      bubble.setAttribute("glow", "#8cc8ff");
      bubble.setAttribute("aria-hidden", "true");
      section.prepend(bubble);
    });
  }

  function install() {
    installStyles();
    installHeroMark();
    installLaunchVideo();
    configureMobileIntroVideo();
    installBackgroundMotion();
  }

  const observer = new MutationObserver(install);
  observer.observe(document.documentElement, { childList: true, subtree: true });
  mobileViewport.addEventListener?.("change", configureMobileIntroVideo);
  install();
})();
