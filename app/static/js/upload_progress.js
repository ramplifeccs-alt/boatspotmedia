document.addEventListener("DOMContentLoaded", function(){
  const form = document.getElementById("uploadForm");
  if (!form) return;

  form.addEventListener("submit", function(e){
    e.preventDefault();

    const fd = new FormData(form);
    const xhr = new XMLHttpRequest();
    const progressBox = document.getElementById("progressBox");
    const progress = document.getElementById("uploadProgress");
    const progressText = document.getElementById("progressText");
    const result = document.getElementById("uploadResult");

    progressBox.style.display = "block";
    result.innerHTML = "";
    progress.style.width = "0%";
    progressText.textContent = "Uploading... 0%";

    xhr.upload.addEventListener("progress", function(evt){
      if (evt.lengthComputable) {
        const percent = Math.round((evt.loaded / evt.total) * 100);
        progress.style.width = percent + "%";
        progressText.textContent = "Uploading... " + percent + "%";
      }
    });

    xhr.onload = function(){
      try {
        const data = JSON.parse(xhr.responseText);
        if (xhr.status >= 200 && xhr.status < 300 && data.ok) {
          progress.style.width = "100%";
          progressText.textContent = "100% complete";
          result.innerHTML = '<div class="notice success">' + data.message + ' <a href="/creator/batches">View Batches</a></div>';
          form.reset();
        } else {
          result.innerHTML = '<div class="notice error">' + (data.error || "Upload failed.") + '</div>';
        }
      } catch (err) {
        result.innerHTML = '<div class="notice error">Upload failed. Server did not return JSON.</div>';
      }
    };

    xhr.onerror = function(){
      result.innerHTML = '<div class="notice error">Upload failed. Please try again.</div>';
    };

    xhr.open("POST", "/creator/upload");
    xhr.send(fd);
  });
});