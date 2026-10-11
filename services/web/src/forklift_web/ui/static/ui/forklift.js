/* Forklift UI behaviour: small progressive enhancements, no build step.

   - Artifact viewers: previews, validation reports and generated schemas live in the object
     store, and the gateway never reads them. The browser asks /api/v1/artifacts/{id}/download
     for a presigned GET (that call checks the raw-rows rules and is audited), fetches the JSON
     from the store and renders it here, always as text (never as HTML).
   - The schema editor has its own scripts (schema-editor.js and the files it names).
   - Copy buttons and confirmations for destructive forms.

   Everything is initialised on page load and again for content HTMX swaps in. */
(function () {
  "use strict";

  function csrfToken() {
    var meta = document.querySelector('meta[name="csrf-token"]');
    return meta ? meta.content : "";
  }

  function apiJSON(url, options) {
    options = options || {};
    var headers = { Accept: "application/json" };
    if (options.body !== undefined) headers["Content-Type"] = "application/json";
    if (options.method && options.method !== "GET") headers["X-CSRFToken"] = csrfToken();
    return fetch(url, {
      method: options.method || "GET",
      body: options.body,
      headers: headers,
      credentials: "same-origin",
    }).then(function (response) {
      return response.text().then(function (text) {
        var body = null;
        try {
          body = text ? JSON.parse(text) : null;
        } catch (ignored) {
          body = null;
        }
        if (!response.ok) {
          var detail = body && body.detail;
          throw new Error(
            typeof detail === "string" ? detail : "The server answered " + response.status + "."
          );
        }
        return body;
      });
    });
  }

  function artifactResponse(downloadUrl) {
    return apiJSON(downloadUrl).then(function (download) {
      return fetch(download.url, { credentials: "omit" }).then(function (response) {
        if (!response.ok) {
          throw new Error("The object store answered " + response.status + " for this file.");
        }
        return response;
      });
    });
  }

  function artifactJSON(downloadUrl) {
    return artifactResponse(downloadUrl).then(function (response) {
      return response.json();
    });
  }

  // The text itself, for JSON whose key order matters (JSON.parse puts "2024" before "id")
  function artifactText(downloadUrl) {
    return artifactResponse(downloadUrl).then(function (response) {
      return response.text();
    });
  }

  window.Forklift = {
    apiJSON: apiJSON,
    artifactJSON: artifactJSON,
    artifactText: artifactText,
    csrfToken: csrfToken,
  };

  function element(tag, text, className) {
    var node = document.createElement(tag);
    if (text !== undefined && text !== null) node.textContent = String(text);
    if (className) node.className = className;
    return node;
  }

  function list(values) {
    if (!values || !values.length) return element("span", "none", "muted");
    return element("span", values.join(", "));
  }

  // ------------------------------------------------------------------ viewers

  function renderPreview(body, data) {
    var columns = data.columns || [];
    var rows = data.rows || [];
    var summary = "First " + rows.length + " row" + (rows.length === 1 ? "" : "s");
    if (data.sheet) summary += " of sheet " + data.sheet;
    if (data.truncated) {
      summary += data.limit === "bytes"
        ? " (the preview reached its size limit; the file has more rows)"
        : " (the file has more rows)";
    }
    body.appendChild(element("p", summary + ".", "muted"));
    if (data.truncated_cells) {
      body.appendChild(
        element("p", data.truncated_cells + " long cells are cut to their first 2000 characters.", "muted")
      );
    }
    var wrap = element("div", null, "table-wrap");
    wrap.tabIndex = 0;
    wrap.setAttribute("role", "region");
    wrap.setAttribute("aria-label", "Preview rows (scrollable)");
    var table = element("table", null, "preview");
    var head = table.createTHead().insertRow();
    head.appendChild(element("th", "#", "num"));
    columns.forEach(function (name) {
      var th = element("th", name);
      th.scope = "col";
      head.appendChild(th);
    });
    var tbody = table.createTBody();
    rows.forEach(function (row, index) {
      var tr = tbody.insertRow();
      var number = element("th", index + 1, "num");
      number.scope = "row";
      tr.appendChild(number);
      row.forEach(function (cell) {
        tr.appendChild(cell === null ? element("td", "null", "null") : element("td", cell));
      });
    });
    wrap.appendChild(table);
    body.appendChild(wrap);
  }

  function fact(dl, label, value) {
    dl.appendChild(element("dt", label));
    var dd = element("dd");
    dd.appendChild(value instanceof Node ? value : element("span", value));
    dl.appendChild(dd);
  }

  function renderReport(body, data) {
    var verdict = element(
      "p",
      data.valid ? "The schema fits this input." : "The schema does not fit this input.",
      data.valid ? "alert ok" : "alert error"
    );
    body.appendChild(verdict);
    if (data.error && data.error.message) {
      body.appendChild(element("p", data.error.code + ": " + data.error.message));
    }
    var dl = element("dl", null, "facts");
    if (data.columns) fact(dl, "Columns in the input", list(data.columns));
    if (data.schema_columns) fact(dl, "Columns in the schema", list(data.schema_columns));
    if (data.required_columns) fact(dl, "Required columns", list(data.required_columns));
    if (data.columns_not_in_schema) {
      fact(dl, "Input columns the schema does not name", list(data.columns_not_in_schema));
    }
    if (data.schema_columns_not_in_input) {
      fact(dl, "Schema columns missing from the input", list(data.schema_columns_not_in_input));
    }
    if (data.sample_rows !== undefined) fact(dl, "Rows checked", data.sample_rows);
    body.appendChild(dl);
  }

  // The text as the engine wrote it: parsing it would put integer-like column names first,
  // and the order of a schema's properties names the columns of a file without a header row
  function renderSchema(body, text) {
    body.appendChild(element("pre", text));
  }

  var RENDERERS = { preview: renderPreview, report: renderReport, schema: renderSchema };
  var AS_TEXT = { schema: true };

  function loadViewer(viewer) {
    if (viewer.dataset.state) return;
    viewer.dataset.state = "loading";
    var status = viewer.querySelector("[data-viewer-status]");
    var body = viewer.querySelector("[data-viewer-body]");
    var button = viewer.querySelector("[data-viewer-load]");
    if (button) button.hidden = true;
    status.textContent = "Loading from the store…";
    (AS_TEXT[viewer.dataset.kind] ? artifactText : artifactJSON)(viewer.dataset.artifactView)
      .then(function (data) {
        body.textContent = "";
        RENDERERS[viewer.dataset.kind](body, data);
        status.textContent = "";
        viewer.dataset.state = "loaded";
      })
      .catch(function (error) {
        status.textContent = "This file could not be shown: " + error.message;
        status.classList.add("error");
        viewer.dataset.state = "failed";
      });
  }

  function initViewer(viewer) {
    if (viewer.dataset.ready) return;
    viewer.dataset.ready = "1";
    var button = viewer.querySelector("[data-viewer-load]");
    if (button) {
      button.hidden = false;
      button.addEventListener("click", function () {
        loadViewer(viewer);
      });
    }
    if (viewer.hasAttribute("data-autoload")) loadViewer(viewer);
  }

  // ------------------------------------------------------------------ copy and confirm

  document.addEventListener("click", function (event) {
    var button = event.target.closest("[data-copy]");
    if (!button) return;
    var source = document.getElementById(button.dataset.copy);
    if (!source || !navigator.clipboard) return;
    navigator.clipboard.writeText(source.textContent.trim()).then(function () {
      var label = button.textContent;
      button.textContent = "Copied";
      setTimeout(function () {
        button.textContent = label;
      }, 2000);
    });
  });

  document.addEventListener(
    "submit",
    function (event) {
      var form = event.target;
      if (form.dataset && form.dataset.confirm && !window.confirm(form.dataset.confirm)) {
        event.preventDefault();
        event.stopImmediatePropagation();
      }
    },
    true
  );

  // ------------------------------------------------------------------ initialisation

  function init(root) {
    root.querySelectorAll("[data-artifact-view]").forEach(initViewer);
    root.querySelectorAll("[data-copy]").forEach(function (button) {
      if (navigator.clipboard) button.hidden = false;
    });
  }

  if (window.htmx) {
    window.htmx.onLoad(init);
  } else {
    document.addEventListener("DOMContentLoaded", function () {
      init(document);
    });
  }
})();
