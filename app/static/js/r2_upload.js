(function(){
  const CHUNK_SIZE = 64 * 1024 * 1024; // 64 MB parts for large camera files.
  let cancelRequested = false;
  let currentBatchId = null;
  let activeXhr = null;

  function $(id){ return document.getElementById(id); }

  function bytesToGB(bytes){ return bytes / 1024 / 1024 / 1024; }

  function formatGB(bytes){
    return bytesToGB(bytes).toFixed(2) + " GB";
  }

  function setStatus(text){
    const el = $("uploadStatus");
    if(el) el.textContent = text;
  }

  function showPanel(){
    const p = $("uploadProgressPanel");
    if(p) p.style.display = "block";
  }

  function updateGlobalProgress(loadedBytes, totalBytes){
    const pct = totalBytes > 0 ? Math.min(100, Math.round((loadedBytes / totalBytes) * 100)) : 0;
    const bar = $("globalBatchProgressBar");
    const text = $("globalBatchProgressText");
    if(bar) bar.style.width = pct + "%";
    if(text) text.textContent = pct + "% (" + formatGB(loadedBytes) + " / " + formatGB(totalBytes) + ")";
  }

  function setCurrentFile(index, total, name){
    const status = $("currentFileStatus");
    const fileName = $("currentFileName");
    if(status) status.textContent = "Uploading file " + index + " of " + total;
    if(fileName) fileName.textContent = name || "";
  }

  function validateFiles(files){
    let total = 0;
    for(const f of files) total += f.size;

    const preview = $("batchSizePreview");
    if(preview){
      preview.textContent = "Selected batch size: " + formatGB(total);
    }

    const maxGB = Number(window.BOATSPOT_UPLOAD_LIMIT_GB || 128);
    const usedGB = Number(window.BOATSPOT_STORAGE_USED_GB || 0);
    const limitGB = Number(window.BOATSPOT_STORAGE_LIMIT_GB || 500);
    const batchGB = bytesToGB(total);

    if(batchGB > maxGB){
      throw new Error("This batch is " + batchGB.toFixed(2) + " GB. Maximum allowed per batch is " + maxGB + " GB.");
    }
    if((usedGB + batchGB) > limitGB){
      throw new Error("This batch exceeds your plan storage. Used: " + usedGB + " GB, selected: " + batchGB.toFixed(2) + " GB, limit: " + limitGB + " GB.");
    }
    return total;
  }

  function postJSON(url, payload){
    return fetch(url, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(payload || {})
    }).then(async r => {
      const data = await r.json().catch(()=>({}));
      if(!r.ok || data.ok === false){
        throw new Error(data.error || data.message || ("Request failed: " + url));
      }
      return data;
    });
  }

  function uploadPUT(url, blob, onProgress){
    return new Promise((resolve, reject)=>{
      const xhr = new XMLHttpRequest();
      activeXhr = xhr;
      xhr.open("PUT", url, true);
      xhr.upload.onprogress = function(e){
        if(e.lengthComputable && onProgress) onProgress(e.loaded, e.total);
      };
      xhr.onload = function(){
        activeXhr = null;
        if(xhr.status >= 200 && xhr.status < 300){
          const etag = xhr.getResponseHeader("ETag") || xhr.getResponseHeader("etag");
          resolve(etag ? etag.replaceAll('"','') : null);
        } else {
          reject(new Error("Upload failed with status " + xhr.status));
        }
      };
      xhr.onerror = function(){
        activeXhr = null;
        reject(new Error("Network error uploading file"));
      };
      xhr.onabort = function(){
        activeXhr = null;
        reject(new Error("Upload cancelled"));
      };
      xhr.send(blob);
    });
  }

  async function uploadMultipart(file, uploadInfo, globalState){
    const totalParts = Math.ceil(file.size / CHUNK_SIZE);
    const parts = [];

    const init = await postJSON("/creator/upload/r2/multipart/init", {
      batch_id: currentBatchId,
      filename: file.name,
      content_type: file.type || "application/octet-stream",
      file_size: file.size,
      key: uploadInfo.key || uploadInfo.r2_key || uploadInfo.r2_video_key
    });

    const uploadId = init.upload_id;
    const key = init.key;

    for(let partNumber = 1; partNumber <= totalParts; partNumber++){
      if(cancelRequested) throw new Error("Upload cancelled by creator.");

      const start = (partNumber - 1) * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const chunk = file.slice(start, end);

      const signed = await postJSON("/creator/upload/r2/multipart/part", {
        upload_id: uploadId,
        key: key,
        part_number: partNumber
      });

      let lastLoaded = 0;
      const etag = await uploadPUT(signed.url, chunk, function(loaded){
        const delta = loaded - lastLoaded;
        lastLoaded = loaded;
        globalState.loaded += delta;
        updateGlobalProgress(globalState.loaded, globalState.total);
      });

      parts.push({PartNumber: partNumber, ETag: etag});
    }

    const completed = await postJSON("/creator/upload/r2/multipart/complete", {
      upload_id: uploadId,
      key: key,
      parts: parts,
      filename: file.name,
      file_size: file.size,
      batch_id: currentBatchId
    });

    return completed;
  }

  async function uploadSingle(file, uploadInfo, globalState){
    const url = uploadInfo.url || uploadInfo.upload_url || uploadInfo.presigned_url;
    if(!url) throw new Error("Upload URL missing for " + file.name);

    let lastLoaded = 0;
    await uploadPUT(url, file, function(loaded){
      const delta = loaded - lastLoaded;
      lastLoaded = loaded;
      globalState.loaded += delta;
      updateGlobalProgress(globalState.loaded, globalState.total);
    });
    return {ok:true};
  }

  async function cancelUpload(){
    cancelRequested = true;
    if(activeXhr) activeXhr.abort();

    const btn = $("cancelUploadBtn");
    if(btn){
      btn.disabled = true;
      btn.textContent = "Cancelling...";
    }

    if(currentBatchId){
      try{
        await fetch("/creator/upload/batch/" + currentBatchId + "/cancel", {method:"POST"});
      }catch(e){}
    }
    alert("Upload cancelled.");
    location.reload();
  }

  document.addEventListener("DOMContentLoaded", function(){
    const filesInput = $("videoFiles");
    const form = $("r2UploadForm");
    const cancelBtn = $("cancelUploadBtn");

    if(cancelBtn) cancelBtn.addEventListener("click", cancelUpload);

    if(filesInput){
      filesInput.addEventListener("change", function(){
        try{
          validateFiles(Array.from(filesInput.files || []));
        }catch(e){
          alert(e.message);
          filesInput.value = "";
        }
      });
    }

    if(!form) return;

    form.addEventListener("submit", async function(e){
      e.preventDefault();
      cancelRequested = false;

      const files = Array.from((filesInput && filesInput.files) || []);
      if(!files.length){
        alert("Choose at least one video file.");
        return;
      }

      let totalBytes = 0;
      try{
        totalBytes = validateFiles(files);
      }catch(err){
        alert(err.message);
        return;
      }

      showPanel();
      updateGlobalProgress(0, totalBytes);
      setStatus("Preparing BoatSpotMedia Storage upload...");

      const submitBtn = $("uploadBatchBtn");
      if(submitBtn) submitBtn.disabled = true;

      const formData = new FormData(form);
      const meta = {
        batch_name: formData.get("batch_name"),
        location: formData.get("location"),
        files: files.map(f => ({name:f.name, size:f.size, type:f.type || "application/octet-stream"}))
      };

      try{
        const prepare = await postJSON("/creator/upload/r2/prepare", meta);
        currentBatchId = prepare.batch_id || prepare.batchId || null;
        window.boatspotCurrentBatchId = currentBatchId;

        const uploads = prepare.uploads || prepare.files || [];
        if(!uploads.length){
          throw new Error("No upload URLs returned by server.");
        }

        const globalState = {loaded:0, total:totalBytes};

        for(let i=0; i<files.length; i++){
          if(cancelRequested) throw new Error("Upload cancelled by creator.");
          const file = files[i];
          const info = uploads[i] || {};
          setCurrentFile(i+1, files.length, file.name);

          if(file.size > CHUNK_SIZE || info.multipart){
            await uploadMultipart(file, info, globalState);
          } else {
            await uploadSingle(file, info, globalState);
          }
        }

        setStatus("Saving video records...");
        await postJSON("/creator/upload/r2/complete", {
          batch_id: currentBatchId,
          files: files.map((f, i)=>({
            filename: f.name,
            file_size: f.size,
            upload: uploads[i] || {}
          }))
        });

        updateGlobalProgress(totalBytes, totalBytes);
        setStatus("Upload complete.");
        const status = $("currentFileStatus");
        if(status) status.textContent = "Upload complete";
        setTimeout(()=>{ window.location.href = "/creator/batches"; }, 900);

      }catch(err){
        if(cancelRequested) return;
        setStatus("");
        alert(err.message || "Upload failed.");
        const panel = $("uploadProgressPanel");
        if(panel) panel.style.display = "block";
        const status = $("currentFileStatus");
        if(status) status.textContent = "Upload failed";
        const fileName = $("currentFileName");
        if(fileName) fileName.textContent = err.message || "";
      }finally{
        if(submitBtn) submitBtn.disabled = false;
      }
    });
  });
})();