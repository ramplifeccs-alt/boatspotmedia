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
    return p === "/creator/batches" || p === "/batches" || p.endsWith("/creator/batches") || p.endsWith("/batches");
  }

  function addButton(){
    if(!shouldShow()) return;
    if(document.getElementById("bsm-delete-latest-incomplete-batch")) return;

    var box = document.createElement("div");
    box.id = "bsm-delete-latest-incomplete-batch";
    box.style.cssText = "margin:12px 0;padding:12px;border:1px solid #fecaca;background:#fff1f2;border-radius:12px;color:#991b1b;font-size:14px;box-shadow:0 4px 14px rgba(0,0,0,.08);";
    box.innerHTML = '<strong>Incomplete / cancelled upload?</strong><br>If a cancelled batch is still showing or storage remains in R2, delete the latest incomplete batch here.<br><button type="button" style="margin-top:8px;background:#dc2626;color:#fff;border:0;border-radius:8px;padding:9px 12px;font-weight:700;">Delete incomplete batch</button>';

    var target = document.querySelector("main") || document.querySelector(".container") || document.querySelector(".content") || document.body;
    target.insertBefore(box, target.firstChild);

    box.querySelector("button").addEventListener("click", async function(){
      if(!confirm("Delete incomplete batch and remove its R2 storage files?")) return;
      try{
        var resp = await fetch("/creator/batch/delete-latest-incomplete", {method:"POST"});
        var data = {};
        try{ data = await resp.json(); }catch(e){}
        if(data.ok){
          alert("Incomplete batch deleted. R2 objects removed: " + (data.deleted_objects || 0));
          location.reload();
        }else{
          alert(data.error || "No incomplete batch found. Nothing was deleted.");
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


/* BoatSpotMedia floating cart button v41.3 */
(function(){
  if(window.__BSM_FLOATING_CART__) return;
  window.__BSM_FLOATING_CART__ = true;
  function addCart(){
    if(document.getElementById("bsm-floating-cart")) return;
    var a = document.createElement("a");
    a.id = "bsm-floating-cart";
    a.href = "/cart";
    a.innerHTML = "🛒";
    a.title = "View cart";
    a.style.position = "fixed";
    a.style.right = "18px";
    a.style.bottom = "18px";
    a.style.width = "54px";
    a.style.height = "54px";
    a.style.borderRadius = "999px";
    a.style.background = "#2563eb";
    a.style.color = "#fff";
    a.style.display = "flex";
    a.style.alignItems = "center";
    a.style.justifyContent = "center";
    a.style.fontSize = "24px";
    a.style.textDecoration = "none";
    a.style.boxShadow = "0 8px 22px rgba(0,0,0,.25)";
    a.style.zIndex = "99999";
    document.body.appendChild(a);
  }
  if(document.readyState === "loading") document.addEventListener("DOMContentLoaded", addCart);
  else addCart();
})();



/* BoatSpotMedia clickable logo global v41.5 */
(function(){
  if(window.__BSM_LOGO_GLOBAL__) return;
  window.__BSM_LOGO_GLOBAL__ = true;
  function logo(){
    var imgHtml = '<img src="/static/img/logo-header.png" alt="BoatSpotMedia" class="bsm-logo-img" style="height:48px;max-width:280px;object-fit:contain;">';
    var candidates = document.querySelectorAll('a, .brand, .navbar-brand, header strong, nav strong');
    candidates.forEach(function(el){
      if((el.textContent || '').trim() === 'BoatSpotMedia' && !el.querySelector('img')){
        if(el.tagName.toLowerCase() === 'a'){
          el.setAttribute('href','/');
          el.innerHTML = imgHtml;
        }else{
          var a = document.createElement('a');
          a.href = '/';
          a.innerHTML = imgHtml;
          el.replaceWith(a);
        }
      }
    });
  }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', logo);
  else logo();
})();

