
(function(){
  function goSettings(e){
    try{
      var el = e.target;
      while(el && el !== document.body){
        var txt = (el.textContent || "").trim().toLowerCase();
        var href = (el.getAttribute && el.getAttribute("href")) || "";
        if(txt === "settings" || href.indexOf("#settings") !== -1 || href.indexOf("dashboard#settings") !== -1){
          e.preventDefault();
          window.location.href = "/creator/settings";
          return false;
        }
        el = el.parentElement;
      }
    }catch(err){}
  }
  document.addEventListener("click", goSettings, true);
})();
