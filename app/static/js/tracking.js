// BoatSpotMedia v50.2 tracking
(function(){
  if(window.__BSM_TRACKING_V502__) return;
  window.__BSM_TRACKING_V502__ = true;

  function send(type, videoId){
    if(!videoId) return;
    try{
      fetch('/track', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        credentials:'same-origin',
        body:JSON.stringify({event_type:type, video_id:videoId, path:location.pathname + location.search})
      }).catch(function(){});
    }catch(e){}
  }

  function markViews(){
    document.querySelectorAll('[data-video-id]').forEach(function(el){
      var id = el.getAttribute('data-video-id');
      if(!id || el.__bsmViewed) return;
      el.__bsmViewed = true;
      send('view', id);
    });
  }

  document.addEventListener('click', function(e){
    var node = e.target.closest('[data-track-click-video-id], [data-video-id] a, a[data-video-id]');
    if(!node) return;
    var id = node.getAttribute('data-track-click-video-id') || node.getAttribute('data-video-id');
    if(!id){
      var parent = node.closest('[data-video-id]');
      if(parent) id = parent.getAttribute('data-video-id');
    }
    if(id) send('click', id);
  }, true);

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', markViews);
  }else{
    markViews();
  }
  setTimeout(markViews, 1500);
})();
