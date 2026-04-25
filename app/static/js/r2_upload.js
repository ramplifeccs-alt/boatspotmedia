document.addEventListener("DOMContentLoaded", function(){
  const form = document.getElementById("r2UploadForm");
  if (!form) return;

  function setResult(html){ document.getElementById("uploadResult").innerHTML = html; }
  function setOverall(pct, text){
    document.getElementById("uploadProgress").style.width = pct + "%";
    document.getElementById("progressText").textContent = text;
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
    setOverall(0, "Preparing direct R2 upload...");

    const metadata = {
      batch_name: form.batch_name.value,
      location: form.location.value,
      original_price: form.original_price.value,
      edited_price: form.edited_price.value,
      bundle_price: form.bundle_price.value,
      files: files.map(f => ({name: f.name, size: f.size, type: f.type || "application/octet-stream"}))
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
          setOverall(overall, "Uploading to R2... " + overall + "%");
        });
        loadedByIndex[i] = file.size;
        bar.style.width = "100%";
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