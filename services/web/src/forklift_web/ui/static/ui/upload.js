/* Uploads from the browser straight into the object store (the gateway never sees the bytes).

   1. POST /api/v1/uploads (session + CSRF token) returns a presigned PUT URL, or, for files
      above the installation's multipart threshold, a multipart upload with part URLs.
   2. The file (or each part, three at a time) is PUT to the store; the progress bar follows
      the bytes sent. Parts whose URL expired (403) or whose connection dropped are retried
      with fresh URLs from POST /api/v1/uploads/{id}/parts.
   3. POST /api/v1/uploads/{id}/complete (with each part's ETag, which the store's CORS rules
      must expose), then on to the upload's page to run it.

   Cancelling aborts the requests and deletes the upload (DELETE /api/v1/uploads/{id}). */
(function () {
  "use strict";

  var form = document.getElementById("upload-form");
  if (!form) return;

  var fileInput = form.querySelector('input[type="file"]');
  var classification = form.querySelector('select[name="classification"]');
  var start = form.querySelector('button[type="submit"]');
  var cancel = document.getElementById("upload-cancel");
  var bar = document.getElementById("upload-progress");
  var amount = document.getElementById("upload-amount");
  var status = document.getElementById("upload-status");
  var urls = form.dataset;
  var PARALLEL = 3;
  var ATTEMPTS = 3;
  var UNITS = ["bytes", "KiB", "MiB", "GiB", "TiB"];

  var requests = [];
  var cancelled = false;
  var uploadId = null;
  var announced = 0;

  function withId(template) {
    return template.replace(urls.placeholder, uploadId);
  }

  function size(bytes) {
    var value = bytes;
    var unit = 0;
    while (value >= 1024 && unit < UNITS.length - 1) {
      value /= 1024;
      unit += 1;
    }
    return unit === 0 ? value + " bytes" : value.toFixed(1) + " " + UNITS[unit];
  }

  function say(text, isError) {
    status.textContent = text;
    status.classList.toggle("error", Boolean(isError));
  }

  function show(sent, total) {
    bar.hidden = false;
    bar.max = total;
    bar.value = sent;
    var percent = total ? Math.floor((100 * sent) / total) : 100;
    amount.textContent = size(sent) + " of " + size(total) + " (" + percent + "%)";
    if (percent >= announced + 25) {
      announced = percent - (percent % 25);
      say("Uploading: " + announced + "% sent.");
    }
  }

  function put(url, body, onProgress) {
    return new Promise(function (resolve, reject) {
      var xhr = new XMLHttpRequest();
      requests.push(xhr);
      xhr.open("PUT", url);
      xhr.upload.addEventListener("progress", function (event) {
        onProgress(event.loaded);
      });
      xhr.addEventListener("load", function () {
        if (xhr.status >= 200 && xhr.status < 300) {
          onProgress(body.size);
          resolve(xhr);
        } else {
          var error = new Error("the store answered " + xhr.status);
          error.status = xhr.status;
          reject(error);
        }
      });
      xhr.addEventListener("error", function () {
        reject(new Error("the connection to the store failed (is its CORS rule set for this site?)"));
      });
      xhr.addEventListener("abort", function () {
        reject(new Error("cancelled"));
      });
      xhr.send(body);
    });
  }

  function uploadParts(ticket, file) {
    var count = ticket.part_count;
    var partSize = ticket.part_size;
    var known = {};
    ticket.parts.forEach(function (part) {
      known[part.part_number] = part.url;
    });
    var loaded = {};
    var etags = [];
    var next = 1;

    function progress(number, bytes) {
      loaded[number] = bytes;
      var sent = 0;
      Object.keys(loaded).forEach(function (key) {
        sent += loaded[key];
      });
      show(sent, file.size);
    }

    function freshUrls(first) {
      var numbers = [];
      for (var n = first; n <= count && numbers.length < 1000; n += 1) numbers.push(n);
      return window.Forklift.apiJSON(withId(urls.partsUrl), {
        method: "POST",
        body: JSON.stringify({ part_numbers: numbers }),
      }).then(function (parts) {
        parts.forEach(function (part) {
          known[part.part_number] = part.url;
        });
      });
    }

    function send(number, attempt) {
      var begin = (number - 1) * partSize;
      var blob = file.slice(begin, Math.min(begin + partSize, file.size));
      var ready = known[number] ? Promise.resolve() : freshUrls(number);
      return ready
        .then(function () {
          return put(known[number], blob, function (bytes) {
            progress(number, bytes);
          });
        })
        .then(function (xhr) {
          var etag = xhr.getResponseHeader("ETag");
          if (!etag) {
            throw new Error(
              "the store did not show the part's ETag; its CORS rule must expose the ETag header"
            );
          }
          etags.push({ part_number: number, etag: etag });
        })
        .catch(function (error) {
          if (cancelled || attempt >= ATTEMPTS || /ETag/.test(error.message)) throw error;
          delete known[number];
          progress(number, 0);
          return send(number, attempt + 1);
        });
    }

    function worker() {
      if (cancelled || next > count) return Promise.resolve();
      var number = next;
      next += 1;
      return send(number, 1).then(worker);
    }

    var workers = [];
    for (var i = 0; i < Math.min(PARALLEL, count); i += 1) workers.push(worker());
    return Promise.all(workers).then(function () {
      return etags.sort(function (a, b) {
        return a.part_number - b.part_number;
      });
    });
  }

  function busy(state) {
    start.disabled = state;
    fileInput.disabled = state;
    classification.disabled = state;
    cancel.hidden = !state;
  }

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    var file = fileInput.files[0];
    if (!file) {
      say("Choose a file first.", true);
      fileInput.focus();
      return;
    }
    cancelled = false;
    uploadId = null;
    announced = 0;
    requests = [];
    busy(true);
    say("Starting the upload of " + file.name + "…");
    window.Forklift.apiJSON(urls.createUrl, {
      method: "POST",
      body: JSON.stringify({
        filename: file.name,
        size: file.size,
        content_type: file.type || "",
        classification: classification.value,
      }),
    })
      .then(function (ticket) {
        uploadId = ticket.upload.id;
        show(0, file.size);
        if (ticket.url) {
          return put(ticket.url, file, function (bytes) {
            show(bytes, file.size);
          }).then(function () {
            return null;
          });
        }
        say("Uploading in " + ticket.part_count + " parts of " + size(ticket.part_size) + ".");
        return uploadParts(ticket, file);
      })
      .then(function (parts) {
        if (cancelled) throw new Error("cancelled");
        say("Checking the upload…");
        return window.Forklift.apiJSON(withId(urls.completeUrl), {
          method: "POST",
          body: JSON.stringify(parts ? { parts: parts } : {}),
        });
      })
      .then(function () {
        say("Upload complete. Opening the file…");
        window.location.assign(withId(urls.detailUrl));
      })
      .catch(function (error) {
        busy(false);
        if (cancelled) {
          say("The upload was cancelled.");
        } else {
          say("The upload failed: " + error.message + ".", true);
        }
      });
  });

  cancel.addEventListener("click", function () {
    cancelled = true;
    requests.forEach(function (xhr) {
      xhr.abort();
    });
    if (uploadId) {
      window.Forklift.apiJSON(withId(urls.deleteUrl), { method: "DELETE" }).catch(function () {
        // the sweeper removes pending uploads that were never completed
      });
    }
  });
})();
