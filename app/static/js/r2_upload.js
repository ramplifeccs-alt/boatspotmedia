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


// BoatSpotMedia enhanced global batch upload progress + cancel cleanup
(function(){
  function qs(id){ return document.getElementById(id); }

  window.BSMUploadProgress = {
    totalBytes: 0,
    uploadedBytes: 0,
    currentFileIndex: 0,
    totalFiles: 0,
    currentFileName: "",
    batchId: null,
    cancelled: false,

    init: function(files, batchId){
      this.totalFiles = files ? files.length : 0;
      this.totalBytes = Array.from(files || []).reduce((s,f)=>s+(f.size||0),0);
      this.uploadedBytes = 0;
      this.currentFileIndex = 0;
      this.batchId = batchId || this.batchId;
      this.cancelled = false;
      this.ensureUI();
      this.render();
    },

    setCurrentFile: function(index, name){
      this.currentFileIndex = index || 0;
      this.currentFileName = name || "";
      this.render();
    },

    addUploaded: function(bytes){
      this.uploadedBytes += bytes || 0;
      if(this.uploadedBytes > this.totalBytes) this.uploadedBytes = this.totalBytes;
      this.render();
    },

    setUploaded: function(bytes){
      this.uploadedBytes = Math.max(0, Math.min(bytes || 0, this.totalBytes || 0));
      this.render();
    },

    percent: function(){
      if(!this.totalBytes) return 0;
      return Math.round((this.uploadedBytes / this.totalBytes) * 100);
    },

    ensureUI: function(){
      let box = qs("bsm-upload-progress-box");
      if(box) return;

      const form = document.querySelector("form") || document.body;
      box = document.createElement("div");
      box.id = "bsm-upload-progress-box";
      box.style.cssText = "margin:18px 0;padding:16px;border:1px solid #dbe3ef;border-radius:14px;background:#fff;box-shadow:0 8px 24px rgba(15,23,42,.08);";
      box.innerHTML = `
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;margin-bottom:10px;">
          <strong id="bsm-upload-title">Batch Upload Progress</strong>
          <button type="button" id="bsm-cancel-upload-btn" style="background:#dc2626;color:white;border:0;border-radius:10px;padding:9px 12px;font-weight:700;">Cancel Upload</button>
        </div>
        <div id="bsm-upload-file" style="font-size:14px;color:#475569;margin-bottom:8px;">Waiting...</div>
        <div style="height:16px;background:#e5e7eb;border-radius:999px;overflow:hidden;">
          <div id="bsm-upload-bar" style="height:100%;width:0%;background:#2563eb;transition:width .2s ease;"></div>
        </div>
        <div id="bsm-upload-percent" style="font-size:13px;color:#475569;margin-top:8px;">0%</div>
      `;
      form.parentNode.insertBefore(box, form.nextSibling);

      qs("bsm-cancel-upload-btn").addEventListener("click", async ()=>{
        this.cancelled = true;
        const bid = this.batchId || window.currentBatchId || window.BSM_CURRENT_BATCH_ID;
        if(bid){
          try{
            await fetch("/upload/batch/" + bid + "/cancel-clean", {method:"POST"});
          }catch(e){}
        }
        alert("Upload cancelled. Uploaded R2 files for this batch were cleaned when possible.");
        location.reload();
      });
    },

    render: function(){
      this.ensureUI();
      const pct = this.percent();
      const bar = qs("bsm-upload-bar");
      const percent = qs("bsm-upload-percent");
      const file = qs("bsm-upload-file");
      if(bar) bar.style.width = pct + "%";
      if(percent) percent.textContent = pct + "% uploaded (" + this.formatBytes(this.uploadedBytes) + " / " + this.formatBytes(this.totalBytes) + ")";
      if(file) file.textContent = this.currentFileName ? ("Uploading file " + this.currentFileIndex + " of " + this.totalFiles + ": " + this.currentFileName) : "Preparing upload...";
    },

    formatBytes: function(bytes){
      if(!bytes) return "0 B";
      const units = ["B","KB","MB","GB","TB"];
      let i=0, n=bytes;
      while(n>=1024 && i<units.length-1){ n/=1024; i++; }
      return n.toFixed(i===0?0:2) + " " + units[i];
    }
  };

  // Hook XHR upload progress globally without replacing existing uploader logic.
  const OldXHR = window.XMLHttpRequest;
  window.XMLHttpRequest = function(){
    const xhr = new OldXHR();
    let lastLoaded = 0;
    if(xhr.upload){
      xhr.upload.addEventListener("progress", function(e){
        if(e.lengthComputable){
          const delta = e.loaded - lastLoaded;
          lastLoaded = e.loaded;
          if(delta > 0 && window.BSMUploadProgress){
            window.BSMUploadProgress.addUploaded(delta);
          }
        }
      });
    }
    return xhr;
  };
})();
