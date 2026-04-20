
function initPanels(){
  document.querySelectorAll('.panel-menu').forEach(menu=>{
    const links=[...menu.querySelectorAll('[data-target]')];
    const shell=menu.closest('.layout')?.querySelector('.panel-shell');
    if(!links.length || !shell) return;
    const sections=[...shell.querySelectorAll('.panel-section[data-panel]')];
    const show=(name)=>{
      sections.forEach(sec=>{
        const active=sec.dataset.panel===name;
        sec.hidden=!active;
        sec.style.display=active?'block':'none';
        sec.classList.toggle('active',active);
      });
      links.forEach(link=>{
        const active = link.dataset.target===name;
        link.classList.toggle('active',active);
        if(active){ link.setAttribute('aria-current','page'); } else { link.removeAttribute('aria-current'); }
      });
    };
    let initial=(window.location.hash||'').replace('#','');
    if(!links.some(l=>l.dataset.target===initial)){ initial=links[0].dataset.target; }
    show(initial);
    links.forEach(link=>{
      link.addEventListener('click',e=>{
        e.preventDefault();
        const name=link.dataset.target;
        show(name);
        try{ window.location.hash=name; }catch(_){}
      });
    });
  });
}
function initPreviews(){
  document.querySelectorAll('[data-preview]').forEach(v=>{
    v.controls=false; v.muted=true; v.loop=true; v.playsInline=true; v.autoplay=true;
    v.setAttribute('controlsList','nodownload noplaybackrate noremoteplayback nofullscreen');
    v.addEventListener('contextmenu',e=>e.preventDefault());
    v.addEventListener('pause',()=>{ try{ v.play(); }catch(e){} });
    try{ v.play().catch(()=>{}); }catch(e){}
  });
}
document.addEventListener('DOMContentLoaded', ()=>{
  initPreviews();
  initPanels();
});
