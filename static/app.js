
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
      });
    };
    let initial=(window.location.hash||'').replace('#','');
    if(!links.some(l=>l.dataset.target===initial)){ initial=links[0].dataset.target; }
    show(initial);
    links.forEach(link=>link.addEventListener('click',e=>{e.preventDefault(); const name=link.dataset.target; show(name); try{window.location.hash=name;}catch(_){};}));
  });
}
function initPreviews(){
  document.querySelectorAll('[data-preview]').forEach(v=>{
    v.controls=false; v.muted=true; v.loop=true; v.playsInline=true; v.autoplay=true;
    try{ v.play().catch(()=>{}); }catch(e){}
  });
}
function initUploadProgress(){
  const uploadForm=document.getElementById('creatorUploadForm');
  const progress=document.getElementById('uploadProgress');
  const status=document.getElementById('uploadStatus');
  if(!uploadForm || !progress || !status) return;
  uploadForm.addEventListener('submit', (e)=>{
    e.preventDefault();
    const formData = new FormData(uploadForm);
    const xhr = new XMLHttpRequest();
    progress.style.display='block';
    progress.value=0;
    status.textContent='Uploading...';
    xhr.upload.addEventListener('progress', evt=>{
      if(evt.lengthComputable){
        const percent=Math.round((evt.loaded/evt.total)*100);
        progress.value=percent;
        status.textContent=`Uploading... ${percent}%`;
      }
    });
    xhr.addEventListener('load', ()=>{
      if(xhr.status>=200 && xhr.status<400){
        progress.value=100;
        status.textContent='Processing preview and thumbnail...';
        window.location.reload();
      } else {
        status.textContent='Upload failed.';
      }
    });
    xhr.addEventListener('error', ()=>{ status.textContent='Upload failed.'; });
    xhr.open('POST', uploadForm.action);
    xhr.send(formData);
  });
}
document.addEventListener('DOMContentLoaded', ()=>{ initPreviews(); initPanels(); initUploadProgress(); });
