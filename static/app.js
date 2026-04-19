
document.addEventListener('DOMContentLoaded', ()=>{
  document.querySelectorAll('[data-preview]').forEach(v=>{
    v.controls=false; v.muted=true; v.loop=true; v.playsInline=true; v.autoplay=true;
    v.addEventListener('contextmenu', e=>e.preventDefault());
    try{ v.play().catch(()=>{});}catch(e){}
  });
  document.querySelectorAll('[data-date-format]').forEach(i=>{
    i.addEventListener('blur', ()=>{
      let digits=i.value.replace(/\D/g,'');
      if(digits.length===8){i.value=`${digits.slice(0,2)}/${digits.slice(2,4)}/${digits.slice(4)}`}
    })
  });
  document.querySelectorAll('[data-time-format]').forEach(i=>{
    i.addEventListener('blur', ()=>{
      let v=i.value.trim().toUpperCase();
      if(/^\d{3,4}$/.test(v)){ let d=v.padStart(4,'0'); let hh=parseInt(d.slice(0,2),10); let mm=d.slice(2); let ampm=hh>=12?'PM':'AM'; hh=((hh+11)%12)+1; i.value=`${hh}:${mm} ${ampm}`; }
    })
  });
});
