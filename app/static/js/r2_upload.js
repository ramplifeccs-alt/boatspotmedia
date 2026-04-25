document.addEventListener("DOMContentLoaded", function(){
  const form = document.getElementById("r2UploadForm");
  if (!form) return;

  function setResult(html){ document.getElementById("uploadResult").innerHTML = html; }
  function setOverall(pct, text){
    document.getElementById("uploadProgress").style.width = pct + "%";
    document.getElementById("progressText").textContent = text;
  }


  function captureMiddleThumbnail(file){
    return new Promise(function(resolve){
      const video = document.createElement("video");
      const url = URL.createObjectURL(file);
      video.preload = "metadata";
      video.muted = true;
      video.playsInline = true;
      video.src = url;

      const cleanup = function(){ URL.revokeObjectURL(url); };

      video.onloadedmetadata = function(){
        const target = isFinite(video.duration) && video.duration > 1 ? video.duration / 2 : 0.1;
        video.currentTime = target;
      };

      video.onseeked = function(){
        try {
          const canvas = document.createElement("canvas");
          const maxW = 640;
          const ratio = video.videoWidth ? maxW / video.videoWidth : 1;
          canvas.width = maxW;
          canvas.height = Math.max(1, Math.round((video.videoHeight || 360) * ratio));
          const ctx = canvas.getContext("2d");
          ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
          canvas.toBlob(function(blob){
            cleanup();
            resolve(blob);
          }, "image/jpeg", 0.78);
        } catch(e) {
          cleanup();
          resolve(null);
        }
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