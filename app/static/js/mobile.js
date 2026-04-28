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


// BoatSpotMedia v38.8 visible ghost batch cleanup button
(function(){
  if(window.__BSM_GHOST_CLEANUP_BUTTON_V388__) return;
  window.__BSM_GHOST_CLEANUP_BUTTON_V388__ = true;

  function shouldShow(){
    var p = window.location.pathname.toLowerCase();
    return p.includes("batch") || p.includes("batches") || document.body.innerText.toLowerCase().includes("batch");
  }

  function addButton(){
    if(!shouldShow()) return;
    if(document.getElementById("bsm-delete-latest-incomplete-batch")) return;

    var box = document.createElement("div");
    box.id = "bsm-delete-latest-incomplete-batch";
    box.style.cssText = "margin:12px 0;padding:12px;border:1px solid #fecaca;background:#fff1f2;border-radius:12px;color:#991b1b;font-size:14px;box-shadow:0 4px 14px rgba(0,0,0,.08);";
    box.innerHTML = '<strong>Incomplete / cancelled upload?</strong><br>If a cancelled batch is still showing or storage remains in R2, delete the latest incomplete batch here.<br><button type="button" style="margin-top:8px;background:#dc2626;color:#fff;border:0;border-radius:8px;padding:9px 12px;font-weight:700;">Delete latest incomplete batch</button>';

    var target = document.querySelector("main") || document.querySelector(".container") || document.querySelector(".content") || document.body;
    target.insertBefore(box, target.firstChild);

    box.querySelector("button").addEventListener("click", async function(){
      if(!confirm("Delete latest incomplete batch and remove its R2 storage files?")) return;
      try{
        var resp = await fetch("/creator/batch/delete-latest-incomplete", {method:"POST"});
        var data = {};
        try{ data = await resp.json(); }catch(e){}
        if(data.ok){
          alert("Incomplete batch deleted. R2 objects removed: " + (data.deleted_objects || 0));
          location.reload();
        }else{
          alert("Could not delete incomplete batch: " + (data.error || "Unknown error"));
        }
      }catch(e){
        alert("Could not delete incomplete batch.");
      }
    });
  }

  if(document.readyState === "loading"){
    document.addEventListener("DOMContentLoaded", addButton);
  }else{
    addButton();
  }
})();
