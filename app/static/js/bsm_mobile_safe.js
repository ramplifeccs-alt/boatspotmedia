(function(){
  if(window.__BSM_MOBILE_SAFE_V505AN__) return;
  window.__BSM_MOBILE_SAFE_V505AN__ = true;

  function addStableMenu(sidebarSelector, buttonClass, label){
    document.querySelectorAll(sidebarSelector).forEach(function(sidebar){
      if(sidebar.querySelector('.' + buttonClass)) return;
      var nav = sidebar.querySelector('nav');
      if(!nav) return;
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.className = buttonClass;
      btn.setAttribute('aria-expanded', 'false');
      btn.textContent = label;
      var logo = sidebar.querySelector('a[href], .bsm-creator-handle-v451, .bsm-creator-handle-v452, .bsm-creator-handle-v453, .bsm-creator-handle-v454, .bsm-creator-handle-v455, .bsm-creator-handle-v457');
      if(logo && logo.parentNode === sidebar){
        logo.insertAdjacentElement('afterend', btn);
      }else{
        sidebar.insertBefore(btn, nav);
      }
      btn.addEventListener('click', function(ev){
        ev.preventDefault();
        ev.stopPropagation();
        var open = sidebar.classList.toggle('bsm-menu-open-v505an');
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        btn.textContent = open ? 'Close Menu' : label;
      });
      nav.addEventListener('click', function(ev){
        ev.stopPropagation();
      });
    });
  }

  function setupPullRefresh(){
    if(document.getElementById('bsm-pull-refresh-v505an')) return;
    var indicator = document.createElement('div');
    indicator.id = 'bsm-pull-refresh-v505an';
    indicator.className = 'bsm-pull-refresh-v505an';
    indicator.textContent = 'Pull to refresh';
    document.body.appendChild(indicator);

    var startY = 0;
    var pulling = false;
    var ready = false;
    var threshold = 86;

    window.addEventListener('touchstart', function(e){
      if(window.scrollY > 1 || !e.touches || e.touches.length !== 1) return;
      startY = e.touches[0].clientY;
      pulling = true;
      ready = false;
    }, {passive:true});

    window.addEventListener('touchmove', function(e){
      if(!pulling || !e.touches || e.touches.length !== 1) return;
      var distance = e.touches[0].clientY - startY;
      if(distance <= 20) return;
      indicator.classList.add('show');
      if(distance > threshold){
        ready = true;
        indicator.classList.add('ready');
        indicator.textContent = 'Release to refresh';
      }else{
        ready = false;
        indicator.classList.remove('ready');
        indicator.textContent = 'Pull to refresh';
      }
    }, {passive:true});

    window.addEventListener('touchend', function(){
      if(!pulling) return;
      pulling = false;
      if(ready){
        indicator.textContent = 'Refreshing...';
        indicator.classList.add('show','ready');
        window.location.reload();
      }else{
        indicator.classList.remove('show','ready');
      }
      ready = false;
    }, {passive:true});
  }

  function init(){
    addStableMenu('.owner-sidebar-v475', 'owner-mobile-toggle-v505an', 'Owner Menu');
    addStableMenu('.bsm-creator-sidebar-v451,.bsm-creator-sidebar-v452,.bsm-creator-sidebar-v453,.bsm-creator-sidebar-v454,.bsm-creator-sidebar-v455,.bsm-creator-sidebar-v457', 'creator-mobile-toggle-v505an', 'Creator Menu');
    setupPullRefresh();
  }

  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
