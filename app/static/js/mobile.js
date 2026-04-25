document.addEventListener("DOMContentLoaded", function () {
  const existing = document.querySelector(".mobile-menu-btn");
  if (!existing) {
    const nav = document.querySelector("nav, .navbar, .topbar");
    if (nav) {
      const btn = document.createElement("button");
      btn.className = "mobile-menu-btn";
      btn.type = "button";
      btn.textContent = "☰";
      btn.setAttribute("aria-label", "Open menu");
      nav.insertBefore(btn, nav.firstChild);
      btn.addEventListener("click", function () {
        nav.classList.toggle("mobile-open");
      });
    }
  }

  document.querySelectorAll(".sidebar").forEach(function (sidebar) {
    if (!sidebar.querySelector(".sidebar-toggle")) {
      const btn = document.createElement("button");
      btn.className = "sidebar-toggle";
      btn.type = "button";
      btn.textContent = "☰ Menu";
      sidebar.insertBefore(btn, sidebar.firstChild);
      btn.addEventListener("click", function () {
        sidebar.classList.toggle("sidebar-open");
      });
    }
  });
});