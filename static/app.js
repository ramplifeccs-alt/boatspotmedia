document.addEventListener('DOMContentLoaded', ()=>{
  document.querySelectorAll('[data-preview]').forEach(v=>{v.controls=false;v.muted=true;v.loop=true;v.playsInline=true;v.autoplay=true;v.addEventListener('contextmenu',e=>e.preventDefault());try{v.play().catch(()=>{});}catch(e){}});
  document.querySelectorAll('[data-time-format]').forEach(i=>{i.addEventListener('blur',()=>{let v=i.value.trim().toUpperCase();if(/^\d{3,4}$/.test(v)){let d=v.padStart(4,'0');let hh=parseInt(d.slice(0,2),10);let mm=d.slice(2);let ampm=hh>=12?'PM':'AM';hh=((hh+11)%12)+1;i.value=`${hh}:${mm} ${ampm}`;}})});

  document.querySelectorAll('.panel-menu').forEach(menu=>{
    const links=[...menu.querySelectorAll('a[data-target]')];
    const shell=menu.closest('.layout')?.querySelector('.panel-shell');
    if(!shell || !links.length) return;
    const sections=[...shell.querySelectorAll('.panel-section[data-panel]')];
    const showPanel=(name)=>{
      sections.forEach(sec=>{const active=sec.dataset.panel===name; sec.hidden=!active; sec.classList.toggle('active',active);});
      links.forEach(link=>link.classList.toggle('active',link.dataset.target===name));
      if(location.hash !== `#${name}`){ history.replaceState(null,'',`#${name}`); }
    };
    const initial=(location.hash||'').replace('#','') || links[0].dataset.target;
    showPanel(initial);
    links.forEach(link=>link.addEventListener('click',e=>{e.preventDefault(); showPanel(link.dataset.target);}));
    window.addEventListener('hashchange',()=>{const target=(location.hash||'').replace('#',''); if(target) showPanel(target);});
  });
});

document.addEventListener('DOMContentLoaded', ()=>{
  document.querySelectorAll('form.time-form').forEach(form=>{
    form.addEventListener('submit', e=>{
      ['from','to'].forEach(prefix=>{
        const h=form.querySelector(`[data-time-hour="${prefix}"]`); const m=form.querySelector(`[data-time-minute="${prefix}"]`); const a=form.querySelector(`[data-time-ampm="${prefix}"]`); const hidden=form.querySelector(`input[name="${prefix}"]`);
        if(h && m && a && hidden){
          const hh=(h.value||'').trim(); const mm=(m.value||'').trim().padStart(2,'0'); const ampm=(a.value||'AM').trim();
          hidden.value = hh && mm ? `${hh}:${mm} ${ampm}` : '';
        }
      });
    });
  });
  document.querySelectorAll('.panel-menu').forEach(menu=>{
    const links=[...menu.querySelectorAll('a[data-target]')];
    const shell=menu.closest('.layout')?.querySelector('.panel-shell');
    if(!shell||!links.length) return;
    const sections=[...shell.querySelectorAll('.panel-section[data-panel]')];
    const show=(name)=>{
      sections.forEach(sec=>{
        const active = sec.dataset.panel===name;
        sec.style.display = active ? 'block' : 'none';
        sec.hidden = !active;
        sec.classList.toggle('active', active);
      });
      links.forEach(link=>link.classList.toggle('active', link.dataset.target===name));
    };
    show(links[0].dataset.target);
    links.forEach(link=>link.addEventListener('click',e=>{e.preventDefault(); show(link.dataset.target);}));
  });
});
