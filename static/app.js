
function clampHour(v){
  const n=parseInt(String(v||'').replace(/\D/g,''),10);
  if(Number.isNaN(n)) return '';
  return String(Math.min(12, Math.max(1, n)));
}
function clampMinute(v){
  const n=parseInt(String(v||'').replace(/\D/g,''),10);
  if(Number.isNaN(n)) return '';
  return String(Math.min(59, Math.max(0, n))).padStart(2,'0');
}

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
      links.forEach(link=>link.classList.toggle('active',link.dataset.target===name));
    };
    const initial=(window.location.hash||'').replace('#','');
    show(links.find(l=>l.dataset.target===initial)?.dataset.target || links[0].dataset.target);
    links.forEach(link=>link.addEventListener('click',e=>{
      e.preventDefault();
      const name=link.dataset.target;
      show(name);
      history.replaceState(null,'',`#${name}`);
    }));
    window.addEventListener('hashchange',()=>{
      const target=(window.location.hash||'').replace('#','');
      if(target) show(target);
    });
  });
}

function initPreviews(){
  document.querySelectorAll('[data-preview]').forEach(v=>{
    v.controls=false; v.muted=true; v.loop=true; v.playsInline=true; v.autoplay=true;
    v.addEventListener('contextmenu',e=>e.preventDefault());
    v.addEventListener('pause',()=>{ try{ v.play(); }catch(e){} });
    try{ v.play().catch(()=>{}); }catch(e){}
  });
}

function initTimeForms(){
  document.querySelectorAll('form.time-form').forEach(form=>{
    ['from','to'].forEach(prefix=>{
      const h=form.querySelector(`[data-time-hour="${prefix}"]`);
      const m=form.querySelector(`[data-time-minute="${prefix}"]`);
      if(h){
        h.addEventListener('input',()=>{ h.value=clampHour(h.value); });
        h.addEventListener('blur',()=>{ h.value=clampHour(h.value); });
      }
      if(m){
        m.addEventListener('input',()=>{ m.value=String(m.value).replace(/\D/g,'').slice(0,2); });
        m.addEventListener('blur',()=>{ m.value=clampMinute(m.value); });
      }
    });
    form.addEventListener('submit', e=>{
      let valid=true;
      ['from','to'].forEach(prefix=>{
        const h=form.querySelector(`[data-time-hour="${prefix}"]`);
        const m=form.querySelector(`[data-time-minute="${prefix}"]`);
        const a=form.querySelector(`[data-time-ampm="${prefix}"]`);
        const hidden=form.querySelector(`input[name="${prefix}"]`);
        const hh=clampHour(h?.value||'');
        const mm=clampMinute(m?.value||'');
        const ap=(a?.value||'AM').trim();
        if(!hh || mm==='') valid=false;
        if(hidden) hidden.value = hh && mm!=='' ? `${hh}:${mm} ${ap}` : '';
        if(h) h.value = hh;
        if(m) m.value = mm;
      });
      if(!valid){ e.preventDefault(); }
    });
  });
}

document.addEventListener('DOMContentLoaded', ()=>{
  initPreviews();
  initPanels();
  initTimeForms();
});
