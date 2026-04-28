document.addEventListener("DOMContentLoaded", function(){
  const form = document.getElementById("r2UploadForm");
  if (!form) return;

  function setResult(html){ document.getElementById("uploadResult").innerHTML = html; }
  function setOverall(pct, text){
    document.getElementById("uploadProgress").style.width = pct + "%";
    document.getElementById("progressText").textContent = text;
  }

  function frameLooksBlack(canvas){
    try{
      const ctx = canvas.getContext("2d");
      const w = Math.min(canvas.width, 80);
      const h = Math.min(canvas.height, 45);
      const data = ctx.getImageData(0,0,w,h).data;
      let total = 0;
      let count = 0;
      for(let i=0;i<data.length;i+=16){
        total += data[i] + data[i+1] + data[i+2];
        count += 3;
      }
      const avg = total / Math.max(count,1);
      return avg < 18;
    }catch(e){ return false; }
  }

  function captureMiddleThumbnail(file){
    return new Promise(function(resolve){
      const video = document.createElement("video");
      const url = URL.createObjectURL(file);
      video.preload = "auto";
      video.muted = true;
      video.playsInline = true;
      video.crossOrigin = "anonymous";
      video.src = url;

      const cleanup = function(){ try{ URL.revokeObjectURL(url); }catch(e){} };

      function captureAt(timeList, index){
        if(index >= timeList.length){
          cleanup();
          return resolve(null);
        }

        const target = timeList[index];
        let done = false;

        const doCapture = function(){
          if(done) return;
          done = true;
          try{
            const maxW = 960;
            const vw = video.videoWidth || 1280;
            const vh = video.videoHeight || 720;
            const ratio = maxW / vw;
            const canvas = document.createElement("canvas");
            canvas.width = maxW;
            canvas.height = Math.max(1, Math.round(vh * ratio));
            const ctx = canvas.getContext("2d");

            // 30% zoom: use center crop of 70% width/height, then draw to full canvas.
            const cropW = vw * 0.70;
            const cropH = vh * 0.70;
            const cropX = (vw - cropW) / 2;
            const cropY = (vh - cropH) / 2;
            ctx.drawImage(video, cropX, cropY, cropW, cropH, 0, 0, canvas.width, canvas.height);

            if(frameLooksBlack(canvas) && index < timeList.length - 1){
              return captureAt(timeList, index + 1);
            }

            canvas.toBlob(function(blob){
              cleanup();
              resolve(blob);
            }, "image/jpeg", 0.82);
          }catch(e){
            captureAt(timeList, index + 1);
          }
        };

        video.onseeked = function(){
          // Give Safari/iPhone a short moment to paint the frame.
          setTimeout(doCapture, 250);
        };
        try{
          video.currentTime = target;
        }catch(e){
          captureAt(timeList, index + 1);
        }
      }

      video.onloadedmetadata = function(){
        const d = isFinite(video.duration) && video.duration > 0 ? video.duration : 10;
        const times = [
          Math.max(0.5, d * 0.50),
          Math.max(0.5, d * 0.45),
          Math.max(0.5, d * 0.55),
          Math.max(0.5, d * 0.60),
          Math.max(0.5, d * 0.35),
          Math.max(0.5, d * 0.70)
        ];
        captureAt(times, 0);
      };

      video.onerror = function(){
        cleanup();
        resolve(null);
      };
    });
  }


  function uploadBlob(blob, uploadUrl, contentType){
    return new Promise(function(resolve, reject){
      if (!blob || !uploadUrl) return resolve(false);
      const xhr = new XMLHttpRequest();
      xhr.open("PUT", uploadUrl);
      xhr.setRequestHeader("Content-Type", contentType || "image/jpeg");
      xhr.onload = function(){
        if (xhr.status >= 200 && xhr.status < 300) resolve(true);
        else reject(new Error("Thumbnail upload failed (HTTP " + xhr.status + ")"));
      };
      xhr.onerror = function(){ reject(new Error("Network error uploading thumbnail")); };
      xhr.send(blob);
    });
  }

  function uploadOne(file, uploadInfo, onProgress){
    return new Promise(function(resolve, reject){
      const xhr = new XMLHttpRequest();
      xhr.open("PUT", uploadInfo.upload_url);
      xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");

      xhr.upload.onprogress = function(evt){
        if (evt.lengthComputable) onProgress(evt.loaded, evt.total);
      };

      xhr.onload = function(){
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve({
            name: uploadInfo.name,
            key: uploadInfo.key,
            size: uploadInfo.size,
            type: uploadInfo.type
          });
        } else {
          reject(new Error("R2 upload failed for " + file.name + " (HTTP " + xhr.status + ")"));
        }
      };
      xhr.onerror = function(){ reject(new Error("Network error uploading " + file.name)); };
      xhr.send(file);
    });
  }

  form.addEventListener("submit", async function(e){
    e.preventDefault();
    setResult("");

    const filesInput = document.getElementById("videoFiles");
    const files = Array.from(filesInput.files || []);
    if (!files.length) {
      setResult('<div class="notice error">Choose at least one video.</div>');
      return;
    }

    const progressBox = document.getElementById("progressBox");
    progressBox.style.display = "block";
    setOverall(0, "Preparing BoatSpotMedia Storage upload...");

    const metadata = {
      batch_name: form.batch_name.value,
      location: form.location.value,
      original_price: form.original_price.value,
      edited_price: form.edited_price.value,
      bundle_price: form.bundle_price.value,
      files: files.map(f => ({name: f.name, size: f.size, type: f.type || "application/octet-stream", last_modified: f.lastModified}))
    };

    let prepare;
    try {
      const res = await fetch("/creator/upload/r2/prepare", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(metadata)
      });
      prepare = await res.json();
      if (!res.ok || !prepare.ok) throw new Error(prepare.error || "Could not prepare upload.");
    } catch(err) {
      setResult('<div class="notice error">' + err.message + '</div>');
      return;
    }

    const uploaded = [];
    const totalBytes = files.reduce((sum, f) => sum + f.size, 0);
    const loadedByIndex = {};
    const list = document.getElementById("fileProgressList");
    list.innerHTML = "";

    try {
      for (let i = 0; i < files.length; i++) {
        if(window.boatspotCancelUpload || boatspotCancelUpload){ throw new Error('Upload cancelled by creator.'); }
        const file = files[i];
        const info = prepare.uploads[i];
        const row = document.createElement("div");
        row.className = "card";
        row.style.marginTop = "10px";
        row.innerHTML = "<strong>" + file.name + "</strong><div class='progress-shell'><div class='progress-bar' style='width:0%'></div></div>";
        list.appendChild(row);
        const bar = row.querySelector(".progress-bar");

        const done = await uploadOne(file, info, function(loaded, total){
          loadedByIndex[i] = loaded;
          const filePct = Math.round((loaded / total) * 100);
          bar.style.width = filePct + "%";

          const loadedTotal = Object.values(loadedByIndex).reduce((a,b) => a + b, 0);
          const overall = Math.round((loadedTotal / totalBytes) * 100);
          setOverall(overall, "Uploading to BoatSpotMedia Storage... " + overall + "%");
        });
        loadedByIndex[i] = file.size;
        bar.style.width = "100%";
        try {
          const thumbBlob = await captureMiddleThumbnail(file);
          if (thumbBlob) {
            await uploadBlob(thumbBlob, info.thumbnail_upload_url, "image/jpeg");
            done.thumbnail_key = info.thumbnail_key;
          }
        } catch(thumbErr) {
          console.warn(thumbErr);
        }
        done.last_modified = file.lastModified;
        uploaded.push(done);
      }

      setOverall(100, "Saving video records...");
      const completeRes = await fetch("/creator/upload/r2/complete", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          batch_id: prepare.batch_id,
          uploaded: uploaded,
          location: metadata.location,
          original_price: metadata.original_price,
          edited_price: metadata.edited_price,
          bundle_price: metadata.bundle_price
        })
      });
      const complete = await completeRes.json();
      if (!completeRes.ok || !complete.ok) throw new Error(complete.error || "Upload completed but records were not saved.");

      setOverall(100, "100% complete");
      setResult('<div class="notice success">' + complete.message + ' <a href="/creator/batches">View Batches</a></div>');
      form.reset();

    } catch(err) {
      setResult('<div class="notice error">' + err.message + '</div>');
    }
  });
});





// BoatSpotMedia v38.5 single visible upload progress controller.
// This does NOT change the thumbnail engine.
(function(){
  if (window.__BSM_UPLOAD_UI_V385__) return;
  window.__BSM_UPLOAD_UI_V385__ = true;

  function $(sel, root){ return (root || document).querySelector(sel); }
  function $all(sel, root){ return Array.from((root || document).querySelectorAll(sel)); }

  function bytes(n){
    n = Number(n || 0);
    if (!n) return "0 B";
    const u = ["B","KB","MB","GB","TB"];
    let i=0;
    while(n >= 1024 && i < u.length - 1){ n /= 1024; i++; }
    return n.toFixed(i ? 2 : 0) + " " + u[i];
  }

  function getFiles(){
    const input = $('input[type="file"]');
    return input && input.files ? Array.from(input.files) : [];
  }

  function getUploadButton(){
    return $('button[type="submit"], input[type="submit"], button#uploadBtn, button[name="upload"], .upload-btn');
  }

  function removeOldDuplicateProgress(){
    // Remove duplicate v38.4/new progress boxes and extra generic boxes, keep our v38.5 box only.
    $all('#bsm-upload-progress-box').forEach(el => el.remove());
    $all('[id="bsm-upload-progress-box"]').forEach(el => el.remove());

    // Hide common old per-file progress areas, but do not remove actual file inputs/forms.
    $all('.upload-progress, .progress-container, .old-upload-progress').forEach(el => {
      if (!el.closest('#bsm-upload-v385')) el.style.display = 'none';
    });

    // Hide progress text from old uploader if it is outside our box and contains "Uploading to BoatSpotMedia".
    $all('div, p, span').forEach(el => {
      const t = (el.textContent || '').trim();
      if (t.startsWith('Uploading to BoatSpotMedia Storage') && !el.closest('#bsm-upload-v385')) {
        el.style.display = 'none';
      }
    });
  }

  function ensureBox(){
    removeOldDuplicateProgress();

    let box = $('#bsm-upload-v385');
    if (box) return box;

    const btn = getUploadButton();
    const form = btn ? btn.closest('form') : $('form');
    box = document.createElement('div');
    box.id = 'bsm-upload-v385';
    box.style.cssText = 'display:none;margin:14px 0 18px 0;padding:16px;border:1px solid #dbe3ef;border-radius:14px;background:#fff;box-shadow:0 8px 24px rgba(15,23,42,.10);';
    box.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px;">
        <div>
          <strong>Batch Upload Progress</strong>
          <div id="bsm-v385-file" style="font-size:13px;color:#475569;margin-top:4px;">Waiting...</div>
        </div>
        <button type="button" id="bsm-v385-cancel" style="background:#dc2626;color:white;border:0;border-radius:10px;padding:9px 12px;font-weight:700;white-space:nowrap;">Cancel Upload</button>
      </div>
      <div style="height:18px;background:#e5e7eb;border-radius:999px;overflow:hidden;">
        <div id="bsm-v385-bar" style="height:100%;width:0%;background:#2563eb;transition:width .15s ease;"></div>
      </div>
      <div id="bsm-v385-text" style="font-size:13px;color:#475569;margin-top:8px;">0% uploaded</div>
    `;

    if (btn && btn.parentNode) {
      // Put it directly after the Upload button so user sees it immediately.
      btn.insertAdjacentElement('afterend', box);
    } else if (form && form.parentNode) {
      form.parentNode.insertBefore(box, form);
    } else {
      document.body.prepend(box);
    }

    $('#bsm-v385-cancel').addEventListener('click', async function(){
      window.__BSM_UPLOAD_CANCELLED__ = true;
      const bid = window.currentBatchId || window.BSM_CURRENT_BATCH_ID || window.current_batch_id || null;
      if (bid) {
        try { await fetch('/r2-clean/batch/' + bid, {method:'POST'}); } catch(e) {}
      }
      alert('Upload cancelled. BoatSpotMedia will clean the uploaded files for this batch when possible.');
      location.reload();
    });

    return box;
  }

  const State = {
    files: [],
    total: 0,
    uploaded: 0,
    currentIndex: 1,
    currentFile: '',
    active: false,
    lastXHRLoaded: new WeakMap()
  };

  function render(){
    const box = ensureBox();
    if (!State.active) return;
    box.style.display = 'block';

    const pct = State.total ? Math.min(100, Math.round((State.uploaded / State.total) * 100)) : 0;
    const bar = $('#bsm-v385-bar');
    const text = $('#bsm-v385-text');
    const file = $('#bsm-v385-file');

    if (bar) bar.style.width = pct + '%';
    if (text) text.textContent = pct + '% uploaded (' + bytes(State.uploaded) + ' / ' + bytes(State.total) + ')';
    if (file) file.textContent = State.currentFile ? ('Uploading file ' + State.currentIndex + ' of ' + State.files.length + ': ' + State.currentFile) : 'Preparing upload...';
  }

  function startUI(){
    State.files = getFiles();
    State.total = State.files.reduce((s,f)=>s + (f.size || 0), 0);
    State.uploaded = 0;
    State.currentIndex = 1;
    State.currentFile = State.files[0] ? State.files[0].name : '';
    State.active = true;

    const btn = getUploadButton();
    if (btn) {
      btn.disabled = true;
      btn.style.opacity = '0.65';
      btn.style.cursor = 'not-allowed';
      btn.dataset.originalText = btn.textContent || btn.value || 'Upload Batch';
      if (btn.tagName === 'INPUT') btn.value = 'Uploading... please wait';
      else btn.textContent = 'Uploading... please wait';
    }

    ensureBox();
    render();

    // Keep box visible if old uploader scrolls/page moves.
    try { ensureBox().scrollIntoView({behavior:'smooth', block:'center'}); } catch(e) {}
  }

  document.addEventListener('submit', function(e){
    const form = e.target;
    if (!form || !form.querySelector('input[type="file"]')) return;
    if (State.active) {
      e.preventDefault();
      e.stopPropagation();
      alert('Upload already in progress. Please wait or use Cancel Upload.');
      return false;
    }
    startUI();
  }, true);

  document.addEventListener('click', function(e){
    const btn = e.target.closest('button, input[type="submit"]');
    if (!btn) return;
    const form = btn.closest('form');
    if (!form || !form.querySelector('input[type="file"]')) return;
    if (!State.active) {
      // Start immediately on click so it is visible before upload requests begin.
      setTimeout(startUI, 0);
    }
  }, true);

  document.addEventListener('change', function(e){
    if (e.target && e.target.matches('input[type="file"]')) {
      State.files = getFiles();
      State.total = State.files.reduce((s,f)=>s + (f.size || 0), 0);
      State.currentFile = State.files[0] ? State.files[0].name : '';
      if (State.files.length) {
        ensureBox().style.display = 'block';
        const text = $('#bsm-v385-text');
        const file = $('#bsm-v385-file');
        if (file) file.textContent = State.files.length + ' file(s) selected';
        if (text) text.textContent = 'Selected batch size: ' + bytes(State.total);
      }
    }
  });

  // XHR progress hook: global bytes, not per-video UI.
  const NativeXHR = window.XMLHttpRequest;
  window.XMLHttpRequest = function(){
    const xhr = new NativeXHR();
    if (xhr.upload) {
      xhr.upload.addEventListener('progress', function(ev){
        if (!State.active || !ev.lengthComputable) return;
        const last = State.lastXHRLoaded.get(xhr) || 0;
        let delta = ev.loaded - last;
        if (delta < 0) delta = ev.loaded;
        State.lastXHRLoaded.set(xhr, ev.loaded);
        State.uploaded += delta;
        if (State.uploaded > State.total && State.total > 0) State.uploaded = State.total;

        // Guess current file by uploaded bytes.
        let running = 0;
        let idx = 1;
        for (let i=0; i<State.files.length; i++){
          running += State.files[i].size || 0;
          if (State.uploaded <= running) { idx = i + 1; break; }
          idx = i + 1;
        }
        State.currentIndex = idx;
        State.currentFile = State.files[idx-1] ? State.files[idx-1].name : State.currentFile;
        render();
      });
      xhr.addEventListener('loadend', function(){
        State.lastXHRLoaded.delete(xhr);
      });
    }
    return xhr;
  };

  // Fetch cannot expose upload progress, but keep UI alive if uploader uses fetch.
  const nativeFetch = window.fetch;
  window.fetch = function(){
    if (State.active) render();
    return nativeFetch.apply(this, arguments).finally(function(){
      if (State.active) render();
    });
  };

  window.BSMUploadProgressV385 = State;
})();



// BoatSpotMedia v38.6 final upload completion + cancel cleanup
(function(){
  if(window.__BSM_V386_FINALIZER__) return;
  window.__BSM_V386_FINALIZER__ = true;

  function findProgressBox(){
    return document.getElementById('bsm-upload-v385') || document.getElementById('bsm-upload-progress-box');
  }

  function finishUploadAndGoBatches(){
    var box = findProgressBox();
    if(box) box.style.display = 'none';
    setTimeout(function(){
      alert('Upload completed successfully. Your files were saved.');
      window.location.href = '/creator/batches';
    }, 250);
  }

  var nativeFetch = window.fetch;
  window.fetch = function(){
    var args = arguments;
    return nativeFetch.apply(this, args).then(function(resp){
      try{
        var url = String(args[0] || '');
        if((url.indexOf('/creator/upload/r2/complete') !== -1 || url.indexOf('/upload/r2/complete') !== -1) && resp && resp.ok){
          setTimeout(finishUploadAndGoBatches, 500);
        }
      }catch(e){}
      return resp;
    });
  };

  document.addEventListener('click', async function(e){
    var btn = e.target.closest('button, a, input');
    if(!btn) return;
    var text = (btn.textContent || btn.value || '').toLowerCase();
    if(text.indexOf('cancel upload') === -1) return;
    var bid = window.currentBatchId || window.BSM_CURRENT_BATCH_ID || window.current_batch_id || null;
    if(bid){
      try{ await fetch('/r2-clean/batch/' + bid, {method:'POST'}); }catch(err){}
    }
  }, true);
})();



// BoatSpotMedia v38.7 completion popup and redirect
(function(){
  if(window.__BSM_V387_COMPLETE__) return;
  window.__BSM_V387_COMPLETE__ = true;
  const oldFetch = window.fetch;
  window.fetch = function(){
    const args = arguments;
    return oldFetch.apply(this, args).then(function(resp){
      try{
        const url = String(args[0] || "");
        if((url.includes("/creator/upload/r2/complete") || url.includes("/upload/r2/complete")) && resp && resp.ok){
          setTimeout(function(){
            const box = document.getElementById("bsm-upload-v385") || document.getElementById("bsm-upload-progress-box");
            if(box) box.style.display = "none";
            alert("Upload completed successfully. Your files were saved.");
            window.location.href = "/creator/batches";
          }, 500);
        }
      }catch(e){}
      return resp;
    });
  };
})();



// BoatSpotMedia v39.1 stronger cancel/delete R2 cleanup
(function(){
  if(window.__BSM_V391_R2_CLEAN__) return;
  window.__BSM_V391_R2_CLEAN__ = true;

  function getBatchId(){
    return window.currentBatchId || window.BSM_CURRENT_BATCH_ID || window.current_batch_id ||
           window.createdBatchId || window.batchId ||
           (document.querySelector("[data-batch-id]") && document.querySelector("[data-batch-id]").getAttribute("data-batch-id"));
  }

  async function cleanBatch(){
    const bid = getBatchId();
    try{
      if(bid){
        return await fetch("/r2-clean/batch/" + bid, {method:"POST"});
      }
      return await fetch("/r2-clean/current-upload", {method:"POST"});
    }catch(e){
      console.warn("R2 cleanup request failed", e);
    }
  }

  document.addEventListener("click", function(e){
    const btn = e.target.closest("button,a,input");
    if(!btn) return;
    const text = (btn.textContent || btn.value || "").toLowerCase();
    if(text.includes("cancel upload") || text.includes("delete batch")){
      cleanBatch();
    }
  }, true);
})();



// BoatSpotMedia v39.3 strong R2 cleanup on cancel/delete
(function(){
  if(window.__BSM_V393_R2_CLEAN__) return;
  window.__BSM_V393_R2_CLEAN__ = true;

  function getBatchId(){
    return window.currentBatchId || window.BSM_CURRENT_BATCH_ID || window.current_batch_id ||
           window.createdBatchId || window.batchId ||
           (document.querySelector("[data-batch-id]") && document.querySelector("[data-batch-id]").getAttribute("data-batch-id"));
  }

  async function cleanBatch(){
    const bid = getBatchId();
    try{
      if(bid){
        return await fetch("/r2-clean/batch/" + bid, {method:"POST"});
      }
      return await fetch("/r2-clean/current-upload", {method:"POST"});
    }catch(e){
      console.warn("R2 cleanup request failed", e);
    }
  }

  document.addEventListener("click", function(e){
    const btn = e.target.closest("button,a,input");
    if(!btn) return;
    const text = (btn.textContent || btn.value || "").toLowerCase();
    if(text.includes("cancel upload") || text.includes("delete batch")){
      cleanBatch();
    }
  }, true);
})();



// BoatSpotMedia v39.4 strong R2 cleanup on cancel/delete
(function(){
  if(window.__BSM_V394_R2_CLEAN__) return;
  window.__BSM_V394_R2_CLEAN__ = true;

  function getBatchId(){
    return window.currentBatchId || window.BSM_CURRENT_BATCH_ID || window.current_batch_id ||
           window.createdBatchId || window.batchId ||
           (document.querySelector("[data-batch-id]") && document.querySelector("[data-batch-id]").getAttribute("data-batch-id"));
  }

  async function cleanBatch(){
    const bid = getBatchId();
    try{
      if(bid){
        return await fetch("/r2-clean/batch/" + bid, {method:"POST"});
      }
      return await fetch("/r2-clean/current-upload", {method:"POST"});
    }catch(e){
      console.warn("R2 cleanup request failed", e);
    }
  }

  document.addEventListener("click", function(e){
    const btn = e.target.closest("button,a,input");
    if(!btn) return;
    const text = (btn.textContent || btn.value || "").toLowerCase();
    if(text.includes("cancel upload") || text.includes("delete batch")){
      cleanBatch();
    }
  }, true);
})();
